/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *    CSI3130 – Symmetric Hash Join Implementation
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "optimizer/clauses.h"
#include "utils/memutils.h"

/* -------------------------------------------------------------------------
 *  CSI3130: SUPPORT FUNCTIONS
 * -------------------------------------------------------------------------
 */

/*
 * Probe a hash table using a given hash value + tuple
 */
static HeapTuple
CSI3130_ProbeHashTable(HashJoinTable table,
                       uint32 hashvalue,
                       TupleTableSlot *probeSlot,
                       List *clauses,
                       ExprContext *econtext,
                       TupleTableSlot *hashTupleSlot)
{
    HashJoinTuple tuple;

    tuple = table->buckets[hashvalue % table->nbuckets];

    while (tuple != NULL)
    {
        if (tuple->hashvalue == hashvalue)
        {
            HeapTuple inner = &tuple->htup;
            TupleTableSlot *innerSlot =
                ExecStoreTuple(inner, hashTupleSlot, InvalidBuffer, false);

            econtext->ecxt_innertuple = innerSlot;

            ResetExprContext(econtext);

            if (ExecQual(clauses, econtext, false))
                return inner;
        }
        tuple = tuple->next;
    }

    return NULL;
}

/* -------------------------------------------------------------------------
 *  ExecHashJoin (CSI3130 REWRITE)
 *
 *  True symmetric hash join:
 *    - Two independent hash tables
 *    - Alternate between inner and outer
 *    - Pipeline: one output per call
 * -------------------------------------------------------------------------
 */
TupleTableSlot *
ExecHashJoin(HashJoinState *state)
{
    EState *estate             = state->js.ps.state;
    ExprContext *econtext      = state->js.ps.ps_ExprContext;

    PlanState *innerNode       = innerPlanState(state);   /* Hash node */
    PlanState *outerNode       = outerPlanState(state);   /* Hash node */

    TupleTableSlot *resultSlot = state->js.ps.ps_ResultTupleSlot;
    TupleTableSlot *innerSlot;
    TupleTableSlot *outerSlot;

    /* Initialize hash tables if needed */
    if (state->sym_innerTable == NULL)
    {
        state->sym_innerTable =
            ExecHashTableCreate((Hash *) innerNode->plan,
                                state->hj_HashOperatorsInner);
    }

    if (state->sym_outerTable == NULL)
    {
        state->sym_outerTable =
            ExecHashTableCreate((Hash *) outerNode->plan,
                                state->hj_HashOperatorsOuter);
    }

    /* ---------------------------
     *  CSI3130: MAIN LOOP
     * --------------------------- */
    for (;;)
    {
        /* ================================================================
         * STEP 1: Process INNER stream (T1)
         * ================================================================ */
        if (!state->sym_exhaustedInner)
        {
            innerSlot = ExecProcNode(innerNode);

            if (TupIsNull(innerSlot))
            {
                state->sym_exhaustedInner = true;
                goto try_outer;
            }

            /* Hash and insert into inner table */
            econtext->ecxt_innertuple = innerSlot;
            uint32 hv = ExecHashGetHashValue(state->sym_innerTable,
                                             econtext,
                                             state->hj_InnerHashKeys);

            HeapTuple tuple = ExecFetchSlotTuple(innerSlot);
            ExecHashTableInsert(state->sym_innerTable, tuple, hv);

            /* Probe OUTER table */
            HeapTuple match = CSI3130_ProbeHashTable(
                    state->sym_outerTable,
                    hv,
                    innerSlot,
                    state->js.joinqual,
                    econtext,
                    state->hj_HashTupleSlot);

            if (match != NULL)
            {
                TupleTableSlot *t =
                    ExecProject(state->js.ps.ps_ProjInfo, NULL);
                return t;
            }

            /* No match — continue looping */
            continue;
        }

try_outer:
        /* ================================================================
         * STEP 2: Process OUTER stream (T2)
         * ================================================================ */
        if (!state->sym_exhaustedOuter)
        {
            outerSlot = ExecProcNode(outerNode);

            if (TupIsNull(outerSlot))
            {
                state->sym_exhaustedOuter = true;
                goto end_join;
            }

            /* Hash and insert into outer table */
            econtext->ecxt_outertuple = outerSlot;

            uint32 hv =
                ExecHashGetHashValue(state->sym_outerTable,
                                     econtext,
                                     state->hj_OuterHashKeys);

            HeapTuple tuple = ExecFetchSlotTuple(outerSlot);
            ExecHashTableInsert(state->sym_outerTable, tuple, hv);

            /* Probe INNER table */
            HeapTuple match = CSI3130_ProbeHashTable(
                    state->sym_innerTable,
                    hv,
                    outerSlot,
                    state->js.joinqual,
                    econtext,
                    state->hj_HashTupleSlot);

            if (match != NULL)
            {
                TupleTableSlot *t =
                    ExecProject(state->js.ps.ps_ProjInfo, NULL);
                return t;
            }

            continue;
        }

        /* ================================================================
         * BOTH SIDES EXHAUSTED → join complete
         * ================================================================ */
end_join:
        return NULL;
    }
}

/* -------------------------------------------------------------------------
 *  ExecInitHashJoin (modified for symmetric hash join)
 * -------------------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate)
{
    HashJoinState *state;
    Plan *outerPlan  = outerPlan(node);
    Hash *innerHash  = (Hash *) innerPlan(node);
    Hash *outerHash  = (Hash *) outerPlan(node);

    state = makeNode(HashJoinState);

    state->js.ps.plan  = (Plan *) node;
    state->js.ps.state = estate;

    ExecAssignExprContext(estate, &state->js.ps);

    /* Initialize targetlist + quals */
    state->js.ps.targetlist =
        ExecInitExpr((Expr *) node->join.plan.targetlist, (PlanState *) state);

    state->js.ps.qual =
        ExecInitExpr((Expr *) node->join.plan.qual, (PlanState *) state);

    state->js.jointype = node->join.jointype;

    state->js.joinqual =
        ExecInitExpr((Expr *) node->join.joinqual, (PlanState *) state);

    state->hashclauses =
        ExecInitExpr((Expr *) node->hashclauses, (PlanState *) state);

    /* Initialize child nodes */
    outerPlanState(state) = ExecInitNode(outerHash->plan.lefttree, estate);
    innerPlanState(state) = ExecInitNode(innerHash->plan.lefttree, estate);

    ExecInitResultTupleSlot(estate, &state->js.ps);

    /* Tuple slot for hash matches */
    HashState *hashstate = (HashState *) innerPlanState(state);
    state->hj_HashTupleSlot = hashstate->ps.ps_ResultTupleSlot;

    ExecAssignResultTypeFromTL(&state->js.ps);
    ExecAssignProjectionInfo(&state->js.ps);

    /* CSI3130: Initialize symmetric join state */
    state->sym_innerTable     = NULL;
    state->sym_outerTable     = NULL;
    state->sym_exhaustedInner = false;
    state->sym_exhaustedOuter = false;

    /* Extract operator lists */
    List *lclauses = NIL;
    List *rclauses = NIL;
    List *hopers   = NIL;
    ListCell *lc;

    foreach(lc, state->hashclauses)
    {
        FuncExprState *fstate = (FuncExprState *) lfirst(lc);
        OpExpr *opexpr        = (OpExpr *) fstate->xprstate.expr;

        lclauses = lappend(lclauses, linitial(fstate->args));
        rclauses = lappend(rclauses, lsecond(fstate->args));
        hopers   = lappend_oid(hopers,    opexpr->opno);
    }

    state->hj_OuterHashKeys       = lclauses;
    state->hj_InnerHashKeys       = rclauses;
    state->hj_HashOperatorsInner  = hopers;
    state->hj_HashOperatorsOuter  = hopers;

    state->js.ps.ps_TupFromTlist = false;

    return state;
}

/* -------------------------------------------------------------------------
 *  ExecEndHashJoin
 * -------------------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *state)
{
    if (state->sym_innerTable)
        ExecHashTableDestroy(state->sym_innerTable);

    if (state->sym_outerTable)
        ExecHashTableDestroy(state->sym_outerTable);

    ExecFreeExprContext(&state->js.ps);

    ExecClearTuple(state->js.ps.ps_ResultTupleSlot);

    ExecEndNode(outerPlanState(state));
    ExecEndNode(innerPlanState(state));
}
