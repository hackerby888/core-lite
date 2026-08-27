#pragma once

// Colony upkeep the node does outside consensus: dropping claims a fork child inherited, and
// deciding which records a background rebuild may take next. Both are pure functions of a colony so
// they can be exercised without a running node.

namespace AntColonyMaintenance
{
// An ant record claimed for a network rebuild sits at ANT_ANN_MATERIALISING until the claiming thread
// publishes or releases it. fork() clones only the calling thread, so a promoted child can inherit a
// claim whose owner never existed there, and ensureAntRecordAnn's waiter would spin on it forever.
inline unsigned int releaseInheritedClaims(AntColonyBpp9000T& colony)
{
    unsigned int released = 0;
    const unsigned int recordCount = colony.solutionCount();
    for (unsigned int index = 0; index < recordCount; index++)
    {
        if (colony.isAnnClaimHeld(index))
        {
            colony.releaseAnnClaim(index);
            released++;
        }
    }
    return released;
}

// A record can be rebuilt only once its parent holds a network, so a scan in commit order - which is
// topological - walks each lineage from the bottom up and never repeats a level.
inline bool isRebuildableNow(AntColonyBpp9000T& colony, unsigned int index)
{
    if (colony.isAnnMaterialised(index) || colony.isAnnClaimHeld(index))
    {
        return false;
    }
    const AntSolutionRecord* record = colony.recordAt(index);
    if (record == nullptr)
    {
        return false;
    }
    if (record->parentRef.isRoot())
    {
        return true;
    }
    const long long parentIndex = colony.findIndexBySolutionRef(record->parentRef);
    return parentIndex != ANT_INVALID_INDEX && colony.isAnnMaterialised((unsigned int)parentIndex);
}
}
