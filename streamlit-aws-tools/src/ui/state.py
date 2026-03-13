# src/ui/state.py
"""
Thin wrappers around ``st.session_state`` that provide consistent, typed access.

Using these helpers instead of direct dict access:
- makes keys easy to discover (centralised calls)
- avoids ``KeyError`` on first render (``ensure_default``)
- keeps page code shorter and intention-clear
"""
from typing import Any, Iterable
import streamlit as st


def get(key: str, default: Any = None) -> Any:
    """Return the session-state value for *key*, or *default* if absent."""
    return st.session_state.get(key, default)


def set(key: str, value: Any) -> None:
    """Write *value* to session state under *key*."""
    st.session_state[key] = value


def ensure_default(key: str, default: Any) -> Any:
    """
    If *key* is not yet in session state, initialise it to *default*.
    Returns the current value (whether just set or already present).
    """
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]


def clear_prefix(prefix: str) -> None:
    """Delete all session-state keys that start with *prefix*."""
    to_delete = [k for k in st.session_state.keys() if k.startswith(prefix)]
    for k in to_delete:
        st.session_state.pop(k, None)


def exists(key: str) -> bool:
    """Return ``True`` if *key* is present in session state."""
    return key in st.session_state
