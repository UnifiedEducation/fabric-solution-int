# Development notes for Sprint 3
A summary of development efforts/ changes made to the Solution repo (and Azure & Fabric), in Sprint 3.

## API Research
- Executed the first 5 steps of the [8-step framework](https://www.skool.com/fabricdojo/classroom/2dbcb373?md=f2691c577b714583bad253a29e13c6ef) for extracting data from REST APIs 
- Created a Google Developers project, and generated an API KEY. 
- Developed a Postman collection of the REST API endpoints of interest, including understanding how authentication works. [Posted here](https://www.skool.com/fabricdojo/classroom/41bb7437?md=d3218e80654e469daf8c4f903f9fb60b). 

## Setting up Azure & Fabric
- Created an Azure Key Vault to store the API Key 
- Designed the Lakehouse RAW layer

## Development begins
- Branch out from `int-dev-datastores`, and developed the Raw Lakehouse, and them PR'd back into int-dev-datastores. 
- Branch out from `int-dev-processing`, develop the Pipeline to query the endpoints, using the AKV reference feature to get API Key from Azure Key Vault, as part of the connection. Add in a Variable Library

## Documentation
- Finalized 8-step framework documentation 
- Updated the architectural diagram with AKV, Pipeline and Lakehouse. 