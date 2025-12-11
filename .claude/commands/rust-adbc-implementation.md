---
description: implement the rust ADBC driver for databricks
---

the high level design doc is in databricks-rust-adbc-driver-design.md file. 
and we've already done the work item planning in detailed-implementation-plan.md file.

for each work item in the detailed-implementation-plan.md file, 
use the workitem-implementation subagent to implement the work item, you should pass the work item id, the planning doc and overall design doc to the subagent.

monitor the subagent, if subagent is not making progress or encounters some errors, please pause and let waitting for instructions