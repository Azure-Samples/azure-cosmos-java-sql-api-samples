---
page_type: sample
languages:
- java
products:
- java sdk
description: "Sample code repo for Azure Cosmos DB Java SDK for SQL API"
urlFragment: ""
---

# Azure Cosmos DB Java SQL API Samples

<!-- 
Guidelines on README format: https://review.docs.microsoft.com/help/onboard/admin/samples/concepts/readme-template?branch=master

Guidance on onboarding samples to docs.microsoft.com/samples: https://review.docs.microsoft.com/help/onboard/admin/samples/process/onboarding?branch=master

Taxonomies for products and languages: https://review.docs.microsoft.com/new-hope/information-architecture/metadata/taxonomies?branch=master
-->

Sample code repo for Azure Cosmos DB Java SDK for SQL API. By cloning and running these samples, and then studying their implementation, you will have an example for sending various requests to Azure Cosmos DB from Java SDK via the SQL API.

## Contents

| File/folder       | Description                                |
|-------------------|--------------------------------------------|
| `src`             | Java sample source code. Many samples have 'sync' and 'async' variants                |
| `.gitignore`      | Define what to ignore at commit time.      |
| `CHANGELOG.md`    | List of changes to the sample.             |
| `CONTRIBUTING.md` | Guidelines for contributing to the sample. |
| `README.md`       | This README file.                          |
| `LICENSE`         | The license for the sample.                |
| `pom.xml`         | Maven Project Object Model File

## Prerequisites

* A Java IDE such as IntelliJ IDEA or VSCode
* Maven
* Setting up an Azure Cosmos DB account through the Azure Portal. The **Create a database account** section of [this guide](https://docs.microsoft.com/en-us/azure/cosmos-db/create-sql-api-java) walks you through account creation. 
* The hostname and master key for your Azure Cosmos DB account

## Setup

Clone the sample to your PC. Using your Java IDE, open pom.xml as a Maven project.

## Running the sample

If you are using Intellij IDEA: Once you have opened the project, go to the **Run/Debug Configurations** drop-down and choose **Edit Configurations**. In the **Edit Configurations** dialog, click **+** (**Add New Configuration**) and give the new configuration a name.
In **Environment variables** paste **ACCOUNT_HOST=** *your account hostname***;ACCOUNT_KEY=** *your account master key* which gives the sample read/write access to your account.

To choose which sample will run, populate the **Main class field** with **com.azure.cosmos.examples.changefeed.***sample* where *sample* can be
* SampleCRUDQuickstart
* SampleCRUDQuickstartAsync
* SampleIndexManagement
* SampleIndexManagementAsync
* SampleStoredProcedure
* SampleStoredProcedureAsync
* Sample ChangeFeedProcessor

## Key concepts

These samples cover a range of Azure Cosmos DB usage topics from more to less basic:
* Basic management of databases, containers and items
* Indexing, stored procedures, and Change Feed
* An end-to-end application sample (*coming soon*)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
