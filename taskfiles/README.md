# Shared Taskfiles

Reusable [Task](https://taskfile.dev/) configurations for common build workflows.

## Available Taskfiles

| Taskfile | Description |
|----------|-------------|
| [`python/poetry-lambda.yml`](./python/poetry-lambda.yml) | Build AWS Lambda packages from Poetry projects |

## Usage

Include a shared Taskfile in your project's `Taskfile.yml`:

```yaml
version: '3'

includes:
  lambda:
    taskfile: https://raw.githubusercontent.com/xoeye/xoi-utils/main/taskfiles/python/poetry-lambda.yml
    vars:
      DIR: ./my-lambda  # Optional: defaults to current directory
```

Then run tasks with the namespace prefix:

```bash
task lambda:bundle    # Build code.zip and layer.zip
task lambda:clean     # Remove build artifacts
```

## Pinning Versions

For reproducible builds, pin to a specific commit or tag:

```yaml
includes:
  lambda:
    taskfile: https://raw.githubusercontent.com/xoeye/xoi-utils/<commit-sha>/taskfiles/python/poetry-lambda.yml
```

Or use a release tag once available:

```yaml
includes:
  lambda:
    taskfile: https://raw.githubusercontent.com/xoeye/xoi-utils/v1.0.0/taskfiles/python/poetry-lambda.yml
```

---

## Python Taskfiles

### poetry-lambda.yml

Builds AWS Lambda deployment packages with code/layer separation:

- **code.zip**: Application code + local editable dependencies
- **layer.zip**: Remote dependencies (for Lambda Layers)

#### Requirements

- [Poetry](https://python-poetry.org/) project with `pyproject.toml`
- [Docker](https://www.docker.com/) (for building the layer with correct platform)
- [Task](https://taskfile.dev/) v3+

#### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OUT` | `./lambda_build` | Output directory for build artifacts |
| `IMAGE` | `public.ecr.aws/sam/build-python3.12` | Docker image for layer build |
| `PLATFORM` | `linux/amd64` | Target platform for Docker build |
| `DIR` | Current directory | Project directory containing `pyproject.toml` |

#### Tasks

| Task | Description |
|------|-------------|
| `bundle` | Main entry point - builds both code.zip and layer.zip |
| `clean` | Remove build artifacts |
| `export` | Export Poetry dependencies to requirements files |
| `build-code` | Build code.zip only |
| `build-layer` | Build layer.zip only |

#### Example

```yaml
# Taskfile.yml
version: '3'

includes:
  api:
    taskfile: https://raw.githubusercontent.com/xoeye/xoi-utils/main/taskfiles/python/poetry-lambda.yml
    vars:
      DIR: ./services/api
      IMAGE: public.ecr.aws/sam/build-python3.11  # Override Python version

  worker:
    taskfile: https://raw.githubusercontent.com/xoeye/xoi-utils/main/taskfiles/python/poetry-lambda.yml
    vars:
      DIR: ./services/worker

tasks:
  build-all:
    desc: Build all Lambda packages
    cmds:
      - task: api:bundle
      - task: worker:bundle
```

Then run:

```bash
task build-all        # Build everything
task api:bundle       # Build just the API lambda
task worker:clean     # Clean worker build artifacts
```

#### Output Structure

After running `task lambda:bundle`:

```
./lambda_build/
├── code.zip              # Upload to Lambda function
├── layer.zip             # Upload to Lambda Layer
├── code/                 # Unpacked code contents
├── layer/                # Unpacked layer contents
├── requirements.txt      # Full Poetry export
├── requirements-remote.txt  # Remote deps (for layer)
└── requirements-local.txt   # Local/editable deps (for code)
```
