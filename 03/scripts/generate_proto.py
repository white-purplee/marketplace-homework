from pathlib import Path
from grpc_tools import protoc
import grpc_tools

ROOT = Path(__file__).resolve().parents[1]
out = ROOT / "generated"
out.mkdir(exist_ok=True)
result = protoc.main([
    "protoc", f"-I{ROOT / 'proto'}",
    f"-I{Path(grpc_tools.__file__).parent / '_proto'}",
    f"--python_out={out}", f"--grpc_python_out={out}",
    str(ROOT / "proto/flight/v1/flight.proto"),
])
if result:
    raise SystemExit(result)
for folder in [out, out / "flight", out / "flight/v1"]:
    (folder / "__init__.py").touch()
print("Generated flight.v1 contract")
