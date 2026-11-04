#pragma once

// AUX trust through the settlement window: a worker records the claimed score instead of walking, and
// the flag lets settle re-walk a result taken on trust before any strict path consumes it.
struct AntSolutionResultLite
{
    AntSolutionResult base;
    unsigned int trusted;
};
static_assert(sizeof(AntSolutionResultLite) <= MAX_SOLUTION_RESULT_BYTES, "AntSolutionResultLite exceeds the settlement result budget");

// The tree record alone. A parent committed on a claimed score has no network yet, which
// resolveAntParent() reports as Absent, and settle never reads the network.
static void resolveAntParentRecord(const AntColonyBpp9000T& colony, const SolutionRef& parentRef, AntParentView& out)
{
    setMem(&out, sizeof(out), 0);

    if (parentRef.isRoot())
    {
        out.status = AntParentReady;
        return;
    }

    const AntSolutionRecord* parentRec = nullptr;
    if (colony.tryGetParent(parentRef, &parentRec) != ValidityResult::Valid || parentRec == nullptr)
    {
        out.status = AntParentAbsent;
        return;
    }

    out.status = AntParentReady;
    out.treeRecord = parentRec;
    out.shift = parentRec->shift;
    out.depth = parentRec->depth;
}
