# Dagster Docs: LLM-Ready

This repository is a **PoC** implementation of **LLM-optimized documentation** for [Dagster OSS](https://dagster.io). The core idea is to break down and curate Dagster’s docs into modular, logically ordered markdown files that can be consumed by an LLM like ChatGPT to build up its contextual knowledge and help it solve specific implementations of Dagster.

---

## 🔧 How to Use This Repo

1. Load the llm-ready markdown files into your LLM **in order**, one file per message.
2. Start with foundational concepts, then progress through more advanced APIs.
3. End with your own **use-case-specific prompt** so the LLM can apply its accumulated knowledge effectively.

| Summary | Prompt it |
|---------|-----------|
| Core Dagster concepts and getting started guide | [![](https://b.lmpify.com/foundation)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dllm-ready%252Ffoundation.md%26pathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Fcore%252Finstallation_quickstart%252F*.md%0A%0AI'm%20new%20to%20Dagster%20and%20want%20to%20understand%20the%20fundamental%20concepts%20and%20how%20to%20get%20started%20building%20data%20pipelines.) |
| Complete guide to building Dagster pipelines with assets | [![](https://b.lmpify.com/build_pipeline)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dllm-ready%252Ffoundation.md%26pathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Fcore%252Fdefining_assets%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Fcore%252Fio_managers_resources_basics%252F*.md%0A%0AHelp%20me%20build%20a%20data%20pipeline%20in%20Dagster%20with%20assets%2C%20dependencies%2C%20and%20proper%20data%20handling.) |
| Scheduling and automation features in Dagster | [![](https://b.lmpify.com/automation)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Forchestration_automation%252Fdeclarative_automation%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Forchestration_automation%252Fschedules_time%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Ffoundation%252Forchestration_automation%252Fsensors_event%252F*.md%0A%0AI%20want%20to%20automate%20my%20Dagster%20pipelines%20with%20schedules%2C%20sensors%2C%20or%20declarative%20automation.) |
| Advanced Dagster features and patterns | [![](https://b.lmpify.com/advanced_patterns)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dllm-ready%252Flayer_one_advanced.md%26pathPatterns%3Dcurated_dagster_docs%252Fadvanced%252Fplatform_patterns%252F**%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Fadvanced%252Ftesting_observability%252F**%252F*.md%0A%0AI%20need%20to%20implement%20advanced%20Dagster%20patterns%20like%20dynamic%20graphs%2C%20asset%20factories%2C%20testing%2C%20or%20debugging.) |
| Deploying and operating Dagster in production | [![](https://b.lmpify.com/deployment)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dcurated_dagster_docs%252Fdeployment_ops%252F**%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Fproject_workflow%252F**%252F*.md%0A%0AHelp%20me%20deploy%20Dagster%20to%20production%20and%20structure%20my%20project%20properly.) |
| End-to-end ETL tutorial with all concepts | [![](https://b.lmpify.com/tutorial_complete)](https://lmpify.com?q=https%3A%2F%2Fuuithub.com%2Fjanwilmake%2Fdagsterdocsexample%2Ftree%2Fmain%3FpathPatterns%3Dllm-ready%252F*.md%26pathPatterns%3Dcurated_dagster_docs%252Fintegrations_examples%252Fetl_tutorial%252F*.md%0A%0AGuide%20me%20through%20building%20a%20complete%20ETL%20pipeline%20in%20Dagster%20from%20start%20to%20finish%20with%20best%20practices.) |

---

## 📁 File Descriptions

### `foundation.md`

- **Purpose**: Introduces the **asset-centric paradigm** that underpins modern Dagster workflows.
- **Contents**: 
  - What assets are
  - How they are materialized
  - How asset dependencies are modeled
  - Benefits of asset-based orchestration
- **Use**: Start with this to give the LLM a strong understanding of Dagster's architectural core.

---

### `layer_one_advanced.md`

- **Purpose**: Builds on the foundation with **practical APIs and features**.
- **Contents**:
  - Ops and Jobs (composable computation)
  - IO Managers (custom input/output behavior)
  - Sensors and Schedules (automation and event-based triggers)
  - Resources and Configuration
- **Use**: Load this file after `foundation.md` to expand the LLM’s understanding of Dagster's building blocks.

---

### `layer_two_api_reference.md`

- **Purpose**: Provides **core API references** and lower-level details.
- **Contents**:
  - `dagster.materialize`, `job`, `op`, `asset`, etc.
  - Execution semantics and type systems
  - Hooks, retries, and execution context
- **Use**: Use this as the third file to cement deep, syntax-level knowledge in the LLM.

---

### `final_use_case_prompt.md`

- **Purpose**: Enables the LLM to **apply its Dagster knowledge** to your specific pipeline.
- **How to Write It**:
  - Include a clear description of your **data source** and **goals**.
  - Share **API documentation**, if available.
  - Be specific about what you want to achieve with Dagster (e.g., automating data ingestion, transforming JSON to Parquet, integrating with dbt).
  - I would highly recommend explicitly building with Polars and DuckDB for local testing.
  - If you have a working or partially working data pipeline, **include code and context**.
- **Use**: This is the final message you provide to the LLM after it has absorbed the three prior files. Your context is added to the prompt before providing to the LLM.

---

## 🧠 Model Strategy

I would recommend using `o4-mini-high` for the first three files, and then `o3` for the last response when you are applying your 

> 💡 *If you're not concerned with usage limits or are on a Pro plan, you may use `o3` for all steps. Otherwise, reserve it for the final step where reasoning is most critical.*

---

## 💬 Tips

LLMs perform best when context is **rich, precise, and structured**. Treat your use case like a mini-spec. The more thoughtful your `final_use_case_prompt.md`, the better the response you will get. 

I would also recommend using [codeprinter](https://github.com/ChristianCasazza/codeprinter) to iterate with the docs, your local code, and the LLMs. It provides an easy way to select the files in a local or github repo, and then provide them to an LLM in organized markdown.

## 💬 Get in Contact

Send me a message on Twitter @casazzany if you have any questions or want to chat.

