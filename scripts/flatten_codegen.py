"""Code generation for sql files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import Union

DBT_MODELS_PATH = Path(__file__).parent / "models"

Child = Union[str, int, float, bool, "Node"]
Node = dict[str, Child]
Branch = list[str]


@dataclass
class QueryLine:
    path: str
    name: str

    def __str__(self) -> str:
        return f"{self.path} as {self.name},"

    @classmethod
    def from_branch(cls, branch: Branch) -> Self:
        path = f"'{branch[0]}'"
        name = ""
        for key in branch[1:-1]:
            path = f"{path} -> '{key}'"

            if key in ("unit", "static", "scenery"):
                continue

            if name != "":
                name = f"{name}_{key}"
            else:
                name = f"{key}"

        path = f"{path} ->> '{branch[-1]}'"

        if name != "":
            name = f"{name}_{branch[-1]}"
        else:
            name = f"{branch[-1]}"
        return QueryLine(path, name)


def walk(
    current: Node | Child, branch: Branch, paths: list[QueryLine]
) -> list[QueryLine]:
    """Walk a dictionaries' keys and produce psql select lines"""
    if (
        isinstance(current, int)
        or isinstance(current, float)
        or isinstance(current, str)
        or isinstance(current, bool)
    ):
        paths.append(QueryLine.from_branch(branch))
        return paths

    for lable, child in current.items():
        branch.append(lable)
        walk(child, branch, paths)
        branch.pop()

    return paths


def test_walk():
    data: Node = {
        "initiator": {
            "unit": {
                "id": 633,
                "fuel": 0,
                "name": "Mineralnye Vody: F-16C #1 (Blue)",
                "type": "F-16C_50",
                "group": {
                    "id": 633,
                    "name": "Mineralnye Vody: F-16C #1 (Blue)",
                    "category": "GROUP_CATEGORY_AIRPLANE",
                    "coalition": "COALITION_BLUE",
                },
                "inAir": False,
                "callsign": "Enfield31",
                "position": {
                    "u": 677014.8036062076,
                    "v": -30099.173251361117,
                    "alt": 546.8906860351562,
                    "lat": 44.44663966697268,
                    "lon": 42.7582882273662,
                },
                "velocity": {
                    "speed": 0,
                    "heading": 0,
                    "velocity": {"x": 0, "y": 0, "z": 0},
                },
                "coalition": "COALITION_BLUE",
                "playerName": "",
                "_playerName": "playerName",
                "orientation": {
                    "up": {
                        "x": -0.026384316384792328,
                        "y": 0.9996215105056763,
                        "z": 0.007793354336172342,
                    },
                    "yaw": -46.68175875066515,
                    "roll": -0.9518056645943377,
                    "pitch": 1.256604534805902,
                    "right": {
                        "x": 0.8046619296073914,
                        "y": 0.016611378639936447,
                        "z": 0.5935008525848389,
                    },
                    "forward": {
                        "x": 0.5931467413902283,
                        "y": 0.02193012833595276,
                        "z": -0.8047956228256226,
                    },
                    "heading": 306.39078523126653,
                },
                "numberInGroup": 1,
            },
            "initiator": "unit",
        }
    }
    lines = walk(data, [], [])
    pprint(list(map(str, lines)))


if __name__ == "__main__":
    test_walk()
