"""python generate.py: модели и обработчики из OpenAPI."""
from pathlib import Path
import subprocess
import sys
import yaml

root = Path(__file__).resolve().parent
spec = yaml.safe_load((root / 'openapi.yaml').read_text())
folder = root / 'generated'
folder.mkdir(exist_ok=True)
(folder / '__init__.py').touch()
subprocess.run([
    sys.executable, '-m', 'datamodel_code_generator', '--input', str(root / 'openapi.yaml'),
    '--input-file-type', 'openapi', '--output', str(folder / 'models.py'),
    '--output-model-type', 'pydantic_v2.BaseModel', '--target-python-version', '3.12',
    '--base-class', 'auth.ApiModel', '--use-standard-collections', '--use-union-operator', '--strict-nullable',
    '--enum-field-as-literal', 'all', '--disable-timestamp',
], check=True)
# Небольшой генератор маршрутов: типы запросов и параметры берутся из контракта.
lines = ['# Generated from openapi.yaml. Do not edit.',
         'from typing import Literal',
         'from fastapi import APIRouter, Depends, Path, Query, Response',
         'from generated import models',
         'from auth import current_user',
         'import service', 'router = APIRouter()', '']
for path, methods in spec['paths'].items():
    for method, op in methods.items():
        name = op['operationId']
        code = next(c for c in op['responses'] if c.startswith('2'))
        response = op['responses'][code].get('content')
        response_type = 'models.' + response['application/json']['schema']['$ref'].split('/')[-1] if response else 'None'
        lines.append(f'@router.{method}({path!r}, status_code={code}, response_model={response_type})')
        args, values = [], []
        if 'requestBody' in op:
            model = op['requestBody']['content']['application/json']['schema']['$ref'].split('/')[-1]
            args.append(f'body: models.{model}')
            values.append('body=body.model_dump()')
        for p in op['parameters']:
            s = p['schema']; key = p['name']
            typ = {'integer':'int', 'string':'str', 'number':'float'}[s['type']]
            if 'enum' in s: typ = 'Literal[' + ', '.join(repr(v) for v in s['enum']) + ']'
            default = '...' if p.get('required') else repr(s.get('default'))
            if default == 'None': typ += ' | None'
            options = [default]
            for source, target in [('minimum','ge'),('maximum','le'),('minLength','min_length'),('maxLength','max_length')]:
                if source in s: options.append(f'{target}={s[source]!r}')
            factory = 'Path' if p['in'] == 'path' else 'Query'
            args.append(f'{key}: {typ} = {factory}({", ".join(options)})')
            values.append(f'{key}={key}')
        if op.get('x-roles'):
            args.append('user: dict = Depends(current_user)')
            values.append('user=user')
        lines.append(f'def {name}({", ".join(args)}):')
        if op.get('x-roles'): lines.append(f'    service.require_role(user, {op["x-roles"]!r})')
        lines.append(f'    result = service.{name}({", ".join(values)})')
        lines.append('    return Response(status_code=204)' if code == '204' else '    return result')
        lines.append('')
(folder / 'routes.py').write_text('\n'.join(lines))
