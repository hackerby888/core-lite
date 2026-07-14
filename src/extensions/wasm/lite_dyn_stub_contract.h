// Each reserved slot gets a fixed state stub until a deployed Wasm module takes over.

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
