// Re-included for each reserved slot, providing a fixed state stub until Wasm deployment.

using namespace QPI;

struct CONTRACT_STATE2_TYPE
{
};

struct CONTRACT_STATE_TYPE : public ContractBase
{
    struct StateData
    {
        Array<uint8, WASM_RESERVED_SLOT_STATE_SIZE> blob;
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
