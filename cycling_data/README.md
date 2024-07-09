# dbt Starter Project Guide

Welcome to the dbt Starter Project! This project is designed to help you get up and running with dbt (Data Build Tool) for transforming data in your analytics warehouse. By following this guide, you'll learn how to execute dbt commands, configure your project, and understand the structure of a typical dbt project.

## Getting Started with dbt

After cloning this repository, you'll want to familiarize yourself with some basic dbt commands:

- `dbt run`: Executes your dbt models.
- `dbt test`: Runs tests on your data to ensure its integrity.
- `dbt build`: A combination command that runs your models, tests, and seeds.

## Project Structure Overview

A dbt project is composed of several key files and directories:

- `dbt_project.yml`: The heart of your dbt project. This YAML file is used to configure your dbt project. Ensure the profile specified here matches the one set up during installation in `~/.dbt/profiles.yml`.
- `*.yml` files in `models`, `data`, `macros` directories: These are documentation files that describe the purpose and structure of the models, datasets, and macros within your project.
- `csv` files in the `data` folder: Serve as the initial data sources for your project. dbt can load these files into your database as tables.
- SQL files in the `models` folder: Contain the SQL queries that define your data models. These models are organized into three layers:
  - **Staging**: Initial preparation and cleaning of raw data.
  - **Core**: Application of business logic.
  - **Datamarts**: Final layer optimized for analysis and reporting.

The relationships between these models are visualized in the lineage graph below:

![Data Model Lineage Graph](/data/lineage_graph.png)

## Workflow Visualization

The following diagram illustrates the workflow from data ingestion to analysis:

![Project Workflow Diagram](/data/workflow.png)

## Detailed Execution Steps

To get your dbt project up and running, follow these detailed steps:

1. **Project Setup**:
   - Navigate to the project directory in your terminal:
     ```
     cd cycling_data
     ```

2. **Data Loading**:
   - Load CSV data into your database, creating tables in your target schema:
     ```
     dbt seed
     ```

3. **Model Execution**:
   - Execute your dbt models to transform the data:
     ```
     dbt run
     ```

4. **Data Testing**:
   - Run tests to ensure data integrity:
        ```
        dbt test
        ```
   - Alternatively, you can use `dbt build` to perform steps 2-4 in a single command.

5. **Documentation Generation**:
   - Generate and view documentation for your project:
     ```
     dbt docs generate
     dbt docs serve
     ```
   - This will host your documentation on a local webserver, typically accessible at http://localhost:8080.

## Additional dbt Resources

For more information on dbt and to become part of the dbt community, check out the following resources:

- [dbt Documentation](https://docs.getdbt.com/docs/introduction): Comprehensive guide to getting started with dbt.
- [dbt Discourse](https://discourse.getdbt.com/): A forum for commonly asked questions and answers.
- [dbt Slack](http://slack.getdbt.com/): Join the dbt community on Slack for live discussions and support.
- [dbt Events](https://events.getdbt.com): Find dbt events and meetups happening around the world.
