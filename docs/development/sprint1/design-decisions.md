> ℹ️ This document provides justification for architectural decisions, in response to PRJ001 🔶 Capacity & Workspace Design (Sprint 1), as part of the Intermediate-Level Fabric Project, in Fabric Dojo.

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

The following naming conventions have been suggested for client review, based on the items found in the High-Level Architecture Diagram are described below:

The general naming convention is described below:
AA_BB_CC_DD
AA = Item Type
BB = Project Code
CC = Deployment Stage
DD = Short Description

Part 1: Item Types:

- FC: Fabric Capacity
- WS: Workspace
- DP: Deployment Pipeline
- SG: Entra ID Security Group
- _More will be added to this list as we progress the project & implementation_

Part 2: Project Code:

- INT: Will be INT for all items in this project - INT is a project code referring to the Intermediate Project in the Dojo.

Part 3: Deployment Stage:

- DEV: Development stage
- TEST: Test stage
- PROD: Production stage
- _Note: the deployment stage is optional, and only needs to be applied if the item goes through a deployment process. For example, in our architecture Security Groups will not be deployed, and so do not need a Deployment Stage in the name_

Part 4: Short Description:

- One or two word description to give people a better idea about the item.

Some examples, including their plain language description:

- **SG_INT_Analysts** - Entra ID Security Group, for the INT project, for Analysts
- **fcintproduction** - a Fabric Capacity, for the INT project, for Prod use cases. - Note: Fabric Capacities must be lower-case and not contain hyphens, so this is a slight exception to the rule.
- **WS_INT_DEV_Processing** - a Fabric Workspace, for the INT project, DEV deployment stage, and inside will be Processing items.
- **DP_INT_Processing** - Deployment Pipeline for the INT project, to manage deployment of items across the three Processing workspaces.

## Other important design decisions not previously mentioned

- The deployment pattern mentioned by the client is closely aligned with [Microsoft's Option 3](https://learn.microsoft.com/en-us/fabric/cicd/manage-deployment#option-3---deploy-using-fabric-deployment-pipelines) and so the high-level architecture has been designed to align with that. More context from the client will be given during the next Sprint, when we'll be focusing on Version Control and Deployment.
