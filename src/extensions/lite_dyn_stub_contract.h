// Deployable-slot stub for runtime dynamic contracts (testnet, LITE_DYNAMIC_CONTRACTS).
// Included once per reserved slot with CONTRACT_INDEX / CONTRACT_STATE_TYPE set, so each
// include generates a distinct struct. Registers nothing live; the host patches this slot's
// dispatch tables at deploy. StateData is a fixed blob sized for any deployed contract.
// See DYNAMIC_CONTRACTS.md.

using namespace QPI;

struct CONTRACT_STATE2_TYPE
{
};

struct CONTRACT_STATE_TYPE : public ContractBase
{
    struct StateData
    {
        Array<uint8, LITE_DYN_SLOT_STATE_SIZE> blob;
    };

    REGISTER_USER_FUNCTIONS_AND_PROCEDURES()
    {
    }

    INITIALIZE()
    {
    }

    BEGIN_EPOCH()
    {
    }

    END_EPOCH()
    {
    }

    BEGIN_TICK()
    {
    }

    END_TICK()
    {
    }
};
