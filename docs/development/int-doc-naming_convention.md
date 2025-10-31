
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