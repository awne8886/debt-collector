# 🧹 Refactor Paginator and Pagination Utilities & Autopurge Feature

🎯 **What:**
- Resolved the code health issue of duplicated `Paginator` definition by extracting pagination utilities from both `main.py` and `bot.py` into a newly created `utils/pagination.py` module.
- In addition to the code health improvement, an explicitly requested feature was implemented: an auto-purge by message content functionality (`!purgemsg` / `/autopurge msg_on`) that allows users to delete and auto-purge messages that contain, start with, or end with specific text.

💡 **Why:**
- The refactoring improves the maintainability and readability of the codebase by consolidating duplicated UI elements and pagination helpers into a single utility file. This simplifies future updates to the view component.
- The new autopurge feature directly fulfills the requested feature enhancements, expanding the existing auto-purge system to act on content filters.

✅ **Verification:**
- Verified that both `main.py` and `bot.py` correctly import the utilities without circular dependencies.
- Successfully executed the existing test suite (`python3 -m unittest discover tests`), which passed without errors after addressing an `await` syntax issue during development.
- Verified standard interaction logic logic on `Paginator.interaction_check` still retrieves `is_superuser` securely via module introspection to avoid cyclic dependencies.

✨ **Result:**
- A cleaner `bot.py` and `main.py` without redundant Paginator implementations.
- A functional and robust `!purgemsg` implementation fully integrated into the existing `_run_purge` and `autopurge_group` logic, including real-time deletion checks via `on_message`.
