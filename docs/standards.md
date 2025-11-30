# Development Standards & Guidelines

## Role & Objectives
This project aims for high-quality, production-grade Python code.

### Core Mandates
-   **Code Quality:** Identify anti-patterns, duplicate logic, and unnecessary complexity.
-   **Validation:** Strict input validation and meaningful error handling.
-   **Testing:** Comprehensive unit and integration tests.
-   **Documentation:** Clear docstrings and architectural documentation.

## Python Coding Standards
-   **Style:** PEP 8, PEP 257.
-   **Typing:** Use type hints everywhere (`typing`, `collections.abc`).
-   **Validation:** Use Pydantic for structured validation.
-   **Logging:** Use `logging` instead of `print`.
-   **Exceptions:** Use structured, custom exceptions.
-   **State:** Avoid global state.

## Architectural Principles
-   **Clean Architecture / DDD:** Separation of concerns.
-   **Immutability:** Favor immutable data structures where possible.
-   **SRP:** Single Responsibility Principle for functions/classes.
-   **Testability:** Design for testability (DI, avoiding heavy mocks).
-   **Processor Extension:** New data transformation logic should extend `app.pipeline.processors.BaseProcessor`, overriding the `transform_record` method. New processors should be registered in `app.worker.PROCESSOR_MAP` for dynamic selection.
