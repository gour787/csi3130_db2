/*-------------------------------------------------------------------------
 *
 * execnodes.h
 *    executor state nodes
 *
 * IDENTIFICATION
 *    $PostgreSQL: pgsql/src/include/nodes/execnodes.h,v 1.123.2.1 2005/11/22 18:23:50 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECNODES_H
#define EXECNODES_H

#include "nodes/lockoptions.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "utils/tuplestore.h"
#include "utils/tuplestore.h"
#include "utils/relcache.h"


/* ----------------------------------------------------------------
 *  Generic PlanState node
 * ----------------------------------------------------------------
 */
typedef struct PlanState
{
    NodeTag     type;
    Plan       *plan;         /* associated Plan */
    EState     *state;        /* at execution time */
    TupleTableSlot *ps_ResultTupleSlot; /* result tuple */
    ExprContext *ps_ExprContext;
    ProjectionInfo *ps_ProjInfo;

    bool        ps_TupFromTlist;
    TupleTableSlot *ps_OuterTupleSlot;
    TupleTableSlot *ps_InnerTupleSlot;

    struct PlanState *lefttree;
    struct PlanState *righttree;

    List       *initPlan;
    List       *qual;
    List       *targetlist;

    int         instrument;
} PlanState;



/* ======================================================================
 *  HASH JOIN STATE (Original PostgreSQL)
 * ======================================================================
 */

typedef struct HashJoinState
{
    JoinState   js;

    /* Saved outer tuple for re-scanning phases */
    TupleTableSlot *hj_FirstOuterTupleSlot;

    /* Single hash table for ORIGINAL hash join algorithm */
    struct HashJoinTableData *hj_HashTable;

    /* Current probe state */
    uint32      hj_CurHashValue;
    int         hj_CurBucketNo;
    struct HashJoinTupleData *hj_CurTuple;

    /* Null tuple for LEFT OUTER join */
    TupleTableSlot *hj_NullInnerTupleSlot;

    /* Outer & Inner key lists */
    List       *hj_OuterHashKeys;
    List       *hj_InnerHashKeys;
    List       *hj_HashOperators;

    /* Flags */
    bool        hj_NeedNewOuter;
    bool        hj_MatchedOuter;
    bool        hj_OuterNotEmpty;

    /* Tuple slots for inner/outer probing */
    TupleTableSlot *hj_OuterTupleSlot;
    TupleTableSlot *hj_HashTupleSlot;

    /* CSI3130 additions appear below */
} HashJoinState;


/* ======================================================================
 *  CSI3130: BEGIN — SYMMETRIC HASH JOIN EXTENSIONS
 * ======================================================================
 */

/*
 * We extend HashJoinState to support symmetric probing.
 * Instead of ONE hash table, we now maintain TWO:
 *
 *  - innerHashTable  (H1)
 *  - outerHashTable  (H2)
 *
 * And we track incremental state for demand-pull execution.
 */

typedef struct SHJState
{
    /* Two hash tables (inner + outer) */
    struct HashJoinTableData *innerHashTable;   /* H1 */
    struct HashJoinTableData *outerHashTable;   /* H2 */

    /* Probe cursor state for each direction */
    int         innerCurBucket;
    int         outerCurBucket;

    struct HashJoinTupleData *innerCurTuple;
    struct HashJoinTupleData *outerCurTuple;

    /* Flags indicating which side the join is currently probing */
    bool        probingInner;      /* true = probing outer hash table */
    bool        probingOuter;      /* true = probing inner hash table */

    /* Saved current tuples for incremental pull */
    TupleTableSlot *curInnerTuple;
    TupleTableSlot *curOuterTuple;

    /* Hash values of active tuple */
    uint32      innerCurHashValue;
    uint32      outerCurHashValue;

    /* CSI3130 requirement: count results */
    long        innerProbes;       /* matches found while probing inner table */
    long        outerProbes;       /* matches found while probing outer table */

    /* Finished flags */
    bool        innerDone;
    bool        outerDone;

} SHJState;

/*
 * We embed the symmetric hash join state directly inside HashJoinState,
 * avoiding the need for new executor node types.
 */
typedef struct HashJoinState HashJoinState;

struct HashJoinState
{
    JoinState   js;

    /* original fields ... (unchanged) */
    TupleTableSlot *hj_FirstOuterTupleSlot;
    struct HashJoinTableData *hj_HashTable;
    uint32      hj_CurHashValue;
    int         hj_CurBucketNo;
    struct HashJoinTupleData *hj_CurTuple;
    TupleTableSlot *hj_NullInnerTupleSlot;
    List       *hj_OuterHashKeys;
    List       *hj_InnerHashKeys;
    List       *hj_HashOperators;
    bool        hj_NeedNewOuter;
    bool        hj_MatchedOuter;
    bool        hj_OuterNotEmpty;
    TupleTableSlot *hj_OuterTupleSlot;
    TupleTableSlot *hj_HashTupleSlot;

    /* CSI 3130: START SYMMETRIC HASH JOIN EXTENSIONS */

    SHJState   *shj;  /* Pointer to symmetric join engine */

    /* CSI 3130: END SYMMETRIC HASH JOIN EXTENSIONS */
};

/* ======================================================================
 *  CSI3130: END — SYMMETRIC HASH JOIN EXTENSIONS
 * ======================================================================
 */

#endif /* EXECNODES_H */
