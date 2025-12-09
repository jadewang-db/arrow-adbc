---
description: implement the rust ADBC driver for databricks
---

the high level design doc is in databricks-rust-adbc-driver-design.md file.
and we've already done the work item planning in detailed-implementation-plan.md file.

for each work item in the detailed-implementation-plan.md file, follow this workflow:

## Workflow for Each Work Item

### 1. Implementation Phase
Use the workitem-implementation subagent to implement the work item:
- Pass the work item id, the planning doc and overall design doc to the subagent
- Monitor the subagent progress
- If subagent is not making progress or encounters errors, pause and wait for instructions

### 2. Validation Phase (REQUIRED)
After implementation completes, use the work-item-validator subagent to verify the work meets all exit criteria:
- Pass the work item description, exit criteria from the planning doc, and recent changes
- The validator will check if all acceptance criteria and requirements are satisfied
- Review the validation report carefully

### 3. Handle Validation Results
Based on the validation outcome:
- **If APPROVED**: Mark work item as complete, proceed to next work item
- **If NEEDS REVISION or INCOMPLETE**:
  * Review the blocking issues and gaps identified
  * Either:
    - Use the workitem-implementation agent again to address the gaps, OR
    - Pause and report to user for guidance on how to proceed
  * Re-validate after fixes are applied

### 4. Progress Tracking
- Keep track of which work items are complete, in progress, or need revision
- Report progress summary after each work item (e.g., "3 of 10 work items complete")
- Do not skip validation - it ensures quality before moving forward

## Important Notes
- Validation is MANDATORY for each work item before proceeding to the next
- Do not batch multiple work items before validation
- If validation repeatedly fails, pause and ask for user guidance