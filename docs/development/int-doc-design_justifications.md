> ℹ️ This document is LIVE and it tracks the design decisions and changes made as we move through the Int-level project in [Fabric Dojo](https://skool.com/fabricdojo/about)


# Sprint 1 Design Decisions
## Specifics on how the design meets each specific client requirement

_Workspaces:_
**[RA001] Your solution must provide separate areas for Processing, Data Stores & Consumption**. & **[RA002] Each of the workspaces above will have a DEV, TEST and PRODUCTION version (so 9 in total).**

- The design accounts for 9 separate workspaces, to separate Processing, Data Stores, and Consumption workloads, through three different Deployment stages (DEV, TEST and PROD) - this results in 9 workspaces in total.
- Three separate deployment pipelines will be used to promote content through each deployment stage. This design was agreed on because it's expected Processing items, Data Stores, and Consumption items will follow different deployment paths (and methods).

_Security:_
**[RA003] Access must be managed at the Workspace level via three Entra ID security groups**: `Engineers`, `Analysts`, `Consumers`

- As requested, access-control will primarily be given at the workspace-level, and Entra ID security groups will be added to the Workspaces.
  - However, the client will be notified on the Auditability tradeoffs of adopting such an approach. Entra ID Security groups make access control _easier_, but it you need to know exactly who was added to a group when, this can become tricky, and will rely on extracting regular security group membership lists.
- However, the client will be informed that if access control requirements change in the future (for example, more granular permissions are required, or the requirement to implement OneLake Security for RLS/ CLS), this approach will need to be thoroughly designed & planned, before implementation.

_Capacities:_
**[RA004] Your solution must isolate production workloads from non-production workloads, so that engineers/ developers working on new features do not throttle a Production capacity** & **[RA005] Costs related to capacities must be kept to an absolute minimum, as such, you should develop a capacity automation strategy (which you will later implement).**

- As requested, the capacity design accounts for two Fabric Capacities.
- Production workspaces will be connected to the Production Capacity (`fcintproduction`), and all other workspaces will be connected to the Non-Production Capacity (`fcintdevtest`).
- Note: Fabric Capacities must be lower-case and not contain hyphens, so that restriction is taken into account in the naming convention.
- **Capacity Automation Strategy**: the following Capacity Automation Strategy has been designed to minimize operational cost of the data platform:
  - By default, all Fabric Capacities will be paused, they can be manually resumed in the Azure Portal, when developers need an active capacity (As per the requirements).
  - The daily load will be orchestrated from outside Fabric (mostly likely via GitHub Actions, although other implementation options for this include Logic Apps, or Azure Runbooks). The `fcintproduction` capacity will be resumed, the daily load pipeline will be triggered remotely, when the pipeline completes, the `fcintproduction` capacity will be paused.
  - To make this a reality, the solution will make use of cross-capacity shortcuts (from the inactive to active Capacities), where required.

_Naming convention:_
**[RA006] The client has requested a solid naming convention strategy for: capacities, workspaces, deployment pipelines.**

Please note: the naming convention has been moved into a separate file in this directory: `int-doc-naming_convention.md` 

## Other important design decisions not previously mentioned

- The deployment pattern mentioned by the client is closely aligned with [Microsoft's Option 3](https://learn.microsoft.com/en-us/fabric/cicd/manage-deployment#option-3---deploy-using-fabric-deployment-pipelines) and so the high-level architecture has been designed to align with that. More context from the client will be given during the next Sprint, when we'll be focusing on Version Control and Deployment.

# Sprint 2 Design Decisions and Changes

