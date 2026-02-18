# src/ui/state.py
from typing import Any, Iterable
import streamlit as st

def get(key: str, default: Any = None) -> Any:
    return st.session_state.get(key, default)

def set(key: str, value: Any) -> None:
    st.session_state[key] = value

def ensure_default(key: str, default: Any) -> Any:
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

def clear_prefix(prefix: str) -> None:
    to_delete = [k for k in st.session_state.keys() if k.startswith(prefix)]
    for k in to_delete:
        st.session_state.pop(k, None)

def exists(key: str) -> bool:
    return key in st.session_state