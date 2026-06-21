"""
Compatibility engine for open-data-platform.

Loads the matrix.yaml spec and answers two questions at each selection step:
  1. Which stacks in this layer are available given what was already selected?
  2. Why is a given stack blocked (if it is)?
"""

from __future__ import annotations

import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

MATRIX_PATH = Path(__file__).parent / "matrix.yaml"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CompatEntry:
    name: str
    layer: str
    reason: str


@dataclass
class Stack:
    name: str
    display: str
    description: str
    warning: Optional[str]
    compatible: list[CompatEntry]
    incompatible: list[CompatEntry]


@dataclass
class Layer:
    name: str
    display: str
    description: str
    stacks: list[Stack]


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _parse_entry(raw: dict) -> CompatEntry:
    return CompatEntry(
        name=raw["name"],
        layer=raw["layer"],
        reason=raw["reason"],
    )


def _parse_stack(raw: dict) -> Stack:
    return Stack(
        name=raw["name"],
        display=raw["display"],
        description=raw["description"],
        warning=raw.get("warning"),
        compatible=[_parse_entry(e) for e in raw.get("compatible", [])],
        incompatible=[_parse_entry(e) for e in raw.get("incompatible", [])],
    )


def _parse_layer(raw: dict) -> Layer:
    return Layer(
        name=raw["name"],
        display=raw["display"],
        description=raw["description"],
        stacks=[_parse_stack(s) for s in raw["stacks"]],
    )


def load_matrix() -> list[Layer]:
    with open(MATRIX_PATH, "r") as f:
        data = yaml.safe_load(f)
    return [_parse_layer(l) for l in data["layers"]]


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

@dataclass
class StackStatus:
    stack: Stack
    available: bool
    # If not available, this is the reason from the blocking incompatible entry
    blocked_reason: Optional[str] = None
    # Populated from the prior selection that caused the block
    blocked_by: Optional[str] = None


def evaluate_layer(
    layer: Layer,
    selections: dict[str, str],
) -> list[StackStatus]:
    """
    For each stack in the layer, determine if it is available given the
    current selections dict: {layer_name -> stack_name}.

    A stack is blocked if:
      - Any already-selected stack in another layer lists this stack
        as incompatible, OR
      - This stack lists any already-selected stack as incompatible.

    Both directions are checked because not every incompatibility is
    declared symmetrically in the matrix.
    """
    matrix = load_matrix()
    layer_index: dict[str, Layer] = {l.name: l for l in matrix}
    stack_index: dict[str, dict[str, Stack]] = {
        l.name: {s.name: s for s in l.stacks} for l in matrix
    }

    results: list[StackStatus] = []

    for stack in layer.stacks:
        blocked_reason: Optional[str] = None
        blocked_by: Optional[str] = None

        # Direction 1: does this stack declare any selected stack as incompatible?
        for incompat in stack.incompatible:
            selected_in_that_layer = selections.get(incompat.layer)
            if selected_in_that_layer == incompat.name:
                blocked_reason = incompat.reason
                blocked_by = f"{incompat.layer}: {incompat.name}"
                break

        # Direction 2: does any selected stack declare this stack as incompatible?
        if not blocked_reason:
            for sel_layer_name, sel_stack_name in selections.items():
                if sel_layer_name == layer.name:
                    continue
                sel_stack = stack_index.get(sel_layer_name, {}).get(sel_stack_name)
                if sel_stack is None:
                    continue
                for incompat in sel_stack.incompatible:
                    if incompat.layer == layer.name and incompat.name == stack.name:
                        blocked_reason = incompat.reason
                        blocked_by = f"{sel_layer_name}: {sel_stack_name}"
                        break
                if blocked_reason:
                    break

        results.append(StackStatus(
            stack=stack,
            available=blocked_reason is None,
            blocked_reason=blocked_reason,
            blocked_by=blocked_by,
        ))

    return results


def get_layer_by_name(name: str) -> Optional[Layer]:
    matrix = load_matrix()
    for layer in matrix:
        if layer.name == name:
            return layer
    return None


def get_layer_order() -> list[str]:
    """Returns layer names in the order they appear in the matrix."""
    matrix = load_matrix()
    return [l.name for l in matrix]
