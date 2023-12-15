#include "workload_sched_condition.h"

namespace doris {

class WorkloadScanRowsCondition : public WorkloadSchedCondition {
public:
    bool eval(std::string str_value) override;

private:
    long _scan_rows_limit;
};

bool WorkloadScanRowsCondition::eval(std::string str_value) {
    long input_value = std::stol(str_value);
    if (input_value > _scan_rows_limit) {
        return true;
    }
    return false;
}
}; // namespace doris