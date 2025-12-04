/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *    Routines to hash relations for hashjoin (modified for CSI3130 Symmetric Hash Join)
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/instrument.h"
#include "executor/nodeHash.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "parser/parse_expr.h"

/* -------------------------------------------------------------------------
 *  CSI3130: Symmetric Hash Join — disable batching always
 * -------------------------------------------------------------------------
 */

static void ExecHashIncreaseNumBatches(HashJoinTable hashtable);

/* ----------------------------------------------------------------
 *  ExecHash  (CSI3130: Implement pipelined hash execution)
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecHash(HashState *node)
{
    /* CSI3130: START — implement pipelined hash node support */

    /*
     * In the original PostgreSQL 8.1.4, ExecHash() always throws:
     *     elog(ERROR, "Hash node does not support ExecProcNode call convention");
     *
     * Symmetric hash join REQUIRES pipelined execution.
     *
     * So we simply fetch one tuple from child, hash it, and return it
     * up the pipeline.
     */

    PlanState      *outerNode   = outerPlanState(node);
    TupleTableSlot *slot        = ExecProcNode(outerNode);

    if (TupIsNull(slot))
        return NULL;  /* end of stream */

    /* Return the tuple directly; the HashJoin node does the actual insertion */
    return slot;

    /* CSI3130: END */
}

/* ----------------------------------------------------------------
 *  MultiExecHash — bulk load all inner tuples (unchanged)
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
    PlanState      *outerNode;
    List           *hashkeys;
    HashJoinTable   hashtable;
    TupleTableSlot *slot;
    ExprContext    *econtext;
    uint32          hashvalue;

    /* instrumentation */
    if (node->ps.instrument)
        InstrStartNode(node->ps.instrument);

    outerNode  = outerPlanState(node);
    hashkeys   = node->hashkeys;
    hashtable  = node->hashtable;
    econtext   = node->ps.ps_ExprContext;

    for (;;)
    {
        slot = ExecProcNode(outerNode);
        if (TupIsNull(slot))
            break;

        hashtable->totalTuples++;

        econtext->ecxt_innertuple = slot;
        hashvalue = ExecHashGetHashValue(hashtable, econtext, hashkeys);

        ExecHashTableInsert(hashtable, ExecFetchSlotTuple(slot), hashvalue);
    }

    if (node->ps.instrument)
        InstrStopNodeMulti(node->ps.instrument, hashtable->totalTuples);

    return NULL;
}

/* ----------------------------------------------------------------
 *  ExecInitHash — unchanged
 * ----------------------------------------------------------------
 */
HashState *
ExecInitHash(Hash *node, EState *estate)
{
    HashState  *hashstate;

    hashstate = makeNode(HashState);
    hashstate->ps.plan   = (Plan *) node;
    hashstate->ps.state  = estate;
    hashstate->hashtable = NULL;
    hashstate->hashkeys  = NIL;

    ExecAssignExprContext(estate, &hashstate->ps);
    ExecInitResultTupleSlot(estate, &hashstate->ps);

    hashstate->ps.targetlist = (List *)
        ExecInitExpr((Expr *) node->plan.targetlist, (PlanState *) hashstate);
    hashstate->ps.qual = (List *)
        ExecInitExpr((Expr *) node->plan.qual, (PlanState *) hashstate);

    outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate);

    ExecAssignResultTypeFromTL(&hashstate->ps);
    hashstate->ps.ps_ProjInfo = NULL;

    return hashstate;
}

int
ExecCountSlotsHash(Hash *node)
{
    return ExecCountSlotsNode(outerPlan(node)) +
           ExecCountSlotsNode(innerPlan(node)) + 1;
}

/* ----------------------------------------------------------------
 *  ExecEndHash — unchanged
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
    ExecFreeExprContext(&node->ps);
    ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *  ExecHashTableCreate — CSI3130: force single batch
 * ----------------------------------------------------------------
 */
HashJoinTable
ExecHashTableCreate(Hash *node, List *hashOperators)
{
    HashJoinTable hashtable;
    Plan         *outerNode;
    int           nbuckets;
    int           nbatch;
    int           nkeys;
    int           i;
    ListCell     *lc;
    MemoryContext oldcxt;

    outerNode = outerPlan(node);

    ExecChooseHashTableSize(outerNode->plan_rows,
                            outerNode->plan_width,
                            &nbuckets,
                            &nbatch);

    /* CSI3130: START — disable multiple batches completely */
    nbatch = 1;
    /* CSI3130: END */

    hashtable = (HashJoinTable) palloc(sizeof(HashJoinTableData));
    hashtable->nbuckets         = nbuckets;
    hashtable->nbatch           = 1;
    hashtable->curbatch         = 0;
    hashtable->nbatch_original  = 1;
    hashtable->nbatch_outstart  = 1;

    /* CSI3130: prevent any batch growth */
    hashtable->growEnabled = false;

    hashtable->totalTuples     = 0;
    hashtable->innerBatchFile  = NULL;
    hashtable->outerBatchFile  = NULL;
    hashtable->spaceUsed       = 0;
    hashtable->spaceAllowed    = work_mem * 1024L;

    /*
     * load hash functions
     */
    nkeys = list_length(hashOperators);
    hashtable->hashfunctions = palloc(nkeys * sizeof(FmgrInfo));

    i = 0;
    foreach(lc, hashOperators)
    {
        Oid hashfn = get_op_hash_function(lfirst_oid(lc));

        if (!OidIsValid(hashfn))
            elog(ERROR, "no hash function for operator %u", lfirst_oid(lc));

        fmgr_info(hashfn, &hashtable->hashfunctions[i]);
        i++;
    }

    /*
     * allocate memory contexts
     */
    hashtable->hashCxt = AllocSetContextCreate(
            CurrentMemoryContext,
            "HashTableContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

    hashtable->batchCxt = AllocSetContextCreate(
            hashtable->hashCxt,
            "HashBatchContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

    oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

    hashtable->buckets =
        (HashJoinTuple *) palloc0(nbuckets * sizeof(HashJoinTuple));

    MemoryContextSwitchTo(oldcxt);

    return hashtable;
}

/* ----------------------------------------------------------------
 *  ExecChooseHashTableSize — unchanged EXCEPT nbatch forced later
 * ----------------------------------------------------------------
 */
void
ExecChooseHashTableSize(double ntuples, int tupwidth,
                        int *numbuckets, int *numbatches)
{
    int      tupsize;
    double   inner_bytes;
    long     hash_bytes;
    int      nbuckets;
    int      nbatch;
    int      i;

    if (ntuples <= 0)
        ntuples = 1000;

    tupsize = MAXALIGN(sizeof(HashJoinTupleData)) +
              MAXALIGN(sizeof(HeapTupleHeaderData)) +
              MAXALIGN(tupwidth);

    inner_bytes = ntuples * tupsize;
    hash_bytes  = work_mem * 1024L;

    if (inner_bytes > hash_bytes)
    {
        long lbuckets = (hash_bytes / tupsize) / 10;

        lbuckets = Min(lbuckets, INT_MAX);
        nbuckets = (int) lbuckets;

        double dbatch = ceil(inner_bytes / hash_bytes);

        dbatch = Min(dbatch, INT_MAX/2);
        nbatch = 2;
        while (nbatch < dbatch)
            nbatch <<= 1;
    }
    else
    {
        nbuckets = (int) ceil(ntuples / 10.0);
        nbatch   = 1;
    }

    static const int primes[] = {
        1033,2063,4111,8219,16417,32779,65539,131111,
        262151,524341,1048589,2097211,4194329,8388619,
        16777289,33554473,67108913,134217773,268435463,536870951
    };

    for (i = 0; i < lengthof(primes); i++)
    {
        if (primes[i] >= nbuckets)
        {
            nbuckets = primes[i];
            break;
        }
    }

    *numbuckets = nbuckets;
    *numbatches = nbatch;
}

/* ----------------------------------------------------------------
 *  ExecHashGetBucketAndBatch — CSI3130: always batch 0
 * ----------------------------------------------------------------
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
                          uint32 hashvalue,
                          int *bucketno,
                          int *batchno)
{
    *bucketno = hashvalue % hashtable->nbuckets;

    /* CSI3130: START — disable batching */
    *batchno = 0;
    /* CSI3130: END */
}

/* ----------------------------------------------------------------
 *  ExecHashTableInsert — unchanged except batch removal
 * ----------------------------------------------------------------
 */
void
ExecHashTableInsert(HashJoinTable hashtable,
                    HeapTuple tuple,
                    uint32 hashvalue)
{
    int bucketno;
    int batchno;

    ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

    /* Always in-memory */
    HashJoinTuple hashTuple;
    int sz = MAXALIGN(sizeof(HashJoinTupleData)) + tuple->t_len;

    hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt, sz);

    hashTuple->hashvalue = hashvalue;
    memcpy(&hashTuple->htup, tuple, sizeof(HeapTupleData));
    hashTuple->htup.t_datamcxt = hashtable->batchCxt;
    hashTuple->htup.t_data = (HeapTupleHeader)
        (((char *) hashTuple) + MAXALIGN(sizeof(HashJoinTupleData)));

    memcpy(hashTuple->htup.t_data, tuple->t_data, tuple->t_len);

    hashTuple->next = hashtable->buckets[bucketno];
    hashtable->buckets[bucketno] = hashTuple;

    hashtable->spaceUsed += sz;

    /* batch growth disabled — do nothing */
}

/* ----------------------------------------------------------------
 *  ExecHashGetHashValue — unchanged
 * ----------------------------------------------------------------
 */
uint32
ExecHashGetHashValue(HashJoinTable hashtable,
                     ExprContext *econtext,
                     List *hashkeys)
{
    uint32      hashkey = 0;
    ListCell   *lc;
    int         i = 0;

    ResetExprContext(econtext);

    MemoryContext old = MemoryContextSwitchTo(
            econtext->ecxt_per_tuple_memory);

    foreach(lc, hashkeys)
    {
        ExprState *keyexpr = (ExprState *) lfirst(lc);
        bool isNull;
        Datum keyval;

        hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

        keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

        if (!isNull)
        {
            uint32 hv =
                DatumGetUInt32(FunctionCall1(&hashtable->hashfunctions[i],
                                             keyval));
            hashkey ^= hv;
        }

        i++;
    }

    MemoryContextSwitchTo(old);

    return hashkey;
}

/* ----------------------------------------------------------------
 *  ExecHashTableDestroy — unchanged except batch files are NULL
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashJoinTable hashtable)
{
    MemoryContextDelete(hashtable->hashCxt);
    pfree(hashtable);
}


/* ----------------------------------------------------------------
 *  ExecReScanHash — unchanged
 * ----------------------------------------------------------------
 */
void
ExecReScanHash(HashState *node, ExprContext *exprCtxt)
{
    if (((PlanState *) node)->lefttree->chgParam == NULL)
        ExecReScan(((PlanState *) node)->lefttree, exprCtxt);
}
