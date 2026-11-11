"""Helpers de presentación para las demos CRUD (salida legible en el video)."""
import json


def titulo(texto: str) -> None:
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")


def paso(op: str, texto: str) -> None:
    print(f"\n--- [{op}] {texto}")


def mostrar(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))
