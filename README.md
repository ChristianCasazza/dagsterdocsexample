# Dagster Docs: LLM-Ready

This repository is a **PoC** implementation of **LLM-optimized documentation** for [Dagster OSS](https://dagster.io). The core idea is to break down and curate Dagster’s docs into modular, logically ordered markdown files that can be consumed by an LLM like ChatGPT to build up its contextual knowledge and help it solve specific implementations of Dagster.

---

## 🔧 How to Use This Repo

1. Load the llm-ready markdown files into your LLM **in order**, one file per message.
2. Start with foundational concepts, then progress through more advanced APIs.
3. End with your own **use-case-specific prompt** so the LLM can apply its accumulated knowledge effectively.

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

