"""Веса сети «человек есть / нет» для дома: `.tflite` → `core/person_model.npz` (`core/person.py`).

Модель — эталон MLPerf Tiny «Visual Wake Words» (MobileNetV1 α = 0,25, вход 96×96 RGB в [0, 1], выход
`[нет, есть]`), лицензия Apache-2.0:
https://github.com/mlcommons/tiny/raw/master/benchmark/training/visual_wake_words/trained_models/vww_96_float.tflite

⚠ Скрипт разработчика, в доме не работает: разбор `.tflite` требует пакета `tflite` (pip), а у дома его
нет и не будет (образ HA на musl, объекты бывают офлайн). Дом читает готовый `.npz` голым numpy.
Веса — float16: вдвое меньше пакет, ответ сети расходится с float32 в четвёртом знаке.

    pip install tflite numpy
    python3 scripts/person_model.py vww_96_float.tflite custom_components/mega_home/core/person_model.npz
"""

import sys

import numpy as np
import tflite
from tflite.ActivationFunctionType import ActivationFunctionType
from tflite.BuiltinOperator import BuiltinOperator
from tflite.Padding import Padding

KINDS = {"CONV_2D": "conv", "DEPTHWISE_CONV_2D": "dw", "AVERAGE_POOL_2D": "pool", "FULLY_CONNECTED": "fc", "SOFTMAX": "softmax"}
SKIP = {"RESHAPE", "QUANTIZE", "DEQUANTIZE"}


def weights(model, graph, index):
    if index < 0:
        return None
    t = graph.Tensors(index)
    raw = model.Buffers(t.Buffer()).DataAsNumpy()
    if isinstance(raw, int) or not len(raw):
        return None
    if t.Type() != 0:
        raise SystemExit("нужна модель float32 (vww_96_float.tflite), а не квантованная")
    return np.frombuffer(raw.tobytes(), np.float32).reshape(t.ShapeAsNumpy())


def main(source: str, target: str) -> None:
    model = tflite.Model.GetRootAsModel(open(source, "rb").read(), 0)
    graph = model.Subgraphs(0)
    names = {v: k for k, v in BuiltinOperator.__dict__.items() if not k.startswith("_")}
    kinds, strides, same, acts, arrays = [], [], [], [], {}
    for i in range(graph.OperatorsLength()):
        op = graph.Operators(i)
        code = model.OperatorCodes(op.OpcodeIndex())
        name = names[max(code.BuiltinCode(), code.DeprecatedBuiltinCode())]
        if name in SKIP:
            continue
        if name not in KINDS:
            raise SystemExit(f"операция {name} исполнителю дома неизвестна")
        inputs = op.InputsAsNumpy()
        stride, pad, act = 1, False, 0
        if name in ("CONV_2D", "DEPTHWISE_CONV_2D"):
            options = (tflite.Conv2DOptions if name == "CONV_2D" else tflite.DepthwiseConv2DOptions)()
            options.Init(op.BuiltinOptions().Bytes, op.BuiltinOptions().Pos)
            stride, pad = options.StrideH(), options.Padding() == Padding.SAME
            act = {ActivationFunctionType.RELU: 1, ActivationFunctionType.RELU6: 2}.get(options.FusedActivationFunction(), 0)
        n = len(kinds)
        if name in ("CONV_2D", "DEPTHWISE_CONV_2D", "FULLY_CONNECTED"):
            arrays[f"w{n}"] = weights(model, graph, inputs[1]).astype(np.float16)
            bias = weights(model, graph, inputs[2]) if len(inputs) > 2 else None
            if bias is not None:
                arrays[f"b{n}"] = bias.astype(np.float16)
        kinds.append(KINDS[name])
        strides.append(stride)
        same.append(pad)
        acts.append(act)
    np.savez_compressed(
        target, kinds=np.array(kinds), strides=np.array(strides, np.int8), same=np.array(same), acts=np.array(acts, np.int8),
        size=np.array(96), person=np.array(1), **arrays,
    )
    print(f"{target}: {len(kinds)} слоёв")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
