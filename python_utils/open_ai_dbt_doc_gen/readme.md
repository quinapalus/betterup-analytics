
# DBT Project Documentation Generator

The DBT Project Documentation Generator is a Python application designed to automate the process of generating missing documentation for Data Build Tool (DBT) projects. It utilizes the power of OpenAI's text generation capabilities to create relevant and detailed descriptions for DBT models and columns.

## Features

-   **DBT Project Ingestion:** The application ingests the `manifest.json` file, which contains essential information about the DBT project models.
    
-   **Model Ordering:** Models are sorted in a way that ensures parent models are processed before their children. This guarantees that when a model's description is being generated, the descriptions of its parent models are already available. This enables description inheritance and maintains accurate and coherent documentation.
    
-   **Missing Documentation Identification:** The application identifies models and columns that lack descriptions, making it easy to pinpoint areas that require documentation.
    
-   **Model Description Generation:** The application uses OpenAI's GPT model to generate missing model descriptions. It takes into account the model name, fields, relationships to other models, and descriptions of parent models for better context.
    
-   **Column Description Generation:** In addition to model descriptions, the application generates descriptions for individual columns within each model. This ensures comprehensive documentation of the project's data schema.
    
-   **Individual YML File Generation:** The application generates separate YAML files for each model in the project. These files are conveniently stored in a 'generated_docs' directory, making it easy to access and customize the generated documentation.
    

## Getting Started

1.  Download the BETTERUP-ANALYTICS Repo, CD to /python_utils/open_ai_dbt_doc_gen/

2.  Run `make installdeps`
    
3.  Update the `manifest.json` file in the project directory with your local DBT  manifest, or grab a copy from the github workflow actions - DBT job.
    
4.  Populate the `.env` file with your OpenAI API key:
    `OPENAI_API_KEY=<YOUR_API_KEY>` 
    Replace `<YOUR_API_KEY>` with your actual OpenAI API key.
   
5.  Run the application: 
    `python main.py` 
    
    The application will generate missing documentation for your DBT project, including model descriptions and column descriptions. The generated documentation will be populated in-place in the existing project located in ../warehouse/**