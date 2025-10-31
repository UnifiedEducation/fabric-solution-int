
The general naming convention is described below:

aa-bb-cc-dd
aa = Item Type
bb = Project Code
cc = Deployment Stage
dd = Short Description

#### Part 1: Item Types:
- fc: Fabric Capacity
- dp: Deployment Pipeline
- sg: Entra ID Security Group
- _More will be added to this list as we progress the project & implementation_
- *Note: for Workspace naming, they will not include the i*

#### Part 2: Project Code

- int: we will be using `int` for all items in this project - `int` is a project code referring to the Intermediate Project in the Dojo.

#### Part 3: Deployment Stage:

- dev: Development stage
- test: Test stage
- prod: Production stage
- _Note: the deployment stage is optional, and only needs to be applied if the item goes through a deployment process. For example, in our architecture Security Groups will not be deployed, and so_

#### Part 4: Short Description:

- One or two word description to give people a better idea about the item.

#### Examples

Some examples, including their plain language description:
- **sg-int-analysts** - Entra ID Security Group, for the INT project, for Analysts
- **fcintproduction** - a Fabric Capacity, for the Advanced project (first Version), for use in Data Engineering-related workspaces, at the DEV deployment stage. Note: Fabric Capacities cannot contain hyphens, so they are somewhat of an exception to the overall rule.
- **int-test-processing** - a Fabric Workspace, for the Intermediate project, TEST Deployment stage, and inside will be Processing items. Because it's a workspace, the Item Type is negated.
-  **dp-int-processing** - Deployment Pipeline for the INT project, to manage deployment of items across the three Processing workspaces.
