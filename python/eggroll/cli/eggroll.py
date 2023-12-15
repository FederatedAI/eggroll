#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import os

import click
from ruamel import yaml
from eggroll.cli.commands import task
from eggroll.deepspeed.sdk_client import EggrollClient
from eggroll.config import Config

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(short_help="Eggroll Client", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--eggroll-properties", type=click.Path(exists=True), help="eggroll properties file"
)
@click.pass_context
def eggroll_cli(ctx, eggroll_properties):
    """
    Eggroll Client
    """
    ctx.ensure_object(dict)
    if ctx.invoked_subcommand == "init":
        return

    with open(os.path.join(os.path.dirname(__file__), "settings.yaml"), "r") as fin:
        cli_config = yaml.safe_load(fin)

    if cli_config.get("ip") and cli_config.get("port"):
        ctx.obj["ip"] = cli_config["ip"]
        ctx.obj["port"] = int(cli_config["port"])
    else:
        raise ValueError('Invalid configuration file. Did you run "flow init"?')

    ctx.obj["initialized"] = cli_config.get("ip") and cli_config.get("port")
    if ctx.obj["initialized"]:
        config = Config()
        config.load_default()
        if eggroll_properties:
            config.load_properties(eggroll_properties)
        config.load_env()
        ctx.obj["client"] = EggrollClient(
            config=config, host=cli_config.get("ip"), port=cli_config.get("port")
        )


@eggroll_cli.command("init", short_help="Eggroll CLI Init Command")
@click.option("--ip", type=click.STRING, help="Eggroll gip address.")
@click.option("--port", type=click.INT, help="Eggroll grpc port.")
@click.pass_context
def initialization(ctx: click.Context, ip, port, **kwargs):
    if (ip is None) and (port is None):
        click.secho("[Error]: Please specify ip or port.\n", fg="red")
        click.echo(ctx.get_help())
        ctx.exit(100)

    config = {}
    if not os.path.exists(path := _get_config_path()):
        click.secho(
            f"[Warn ]: config file not found in {path}, will create one.", fg="yellow"
        )
    else:
        with open(_get_config_path(), "r") as fin:
            config = yaml.safe_load(fin)

    _update = False

    def _update_config(_config, _key, _value):
        if (old_value := _config.get(_key)) != _value:
            _config[_key] = _value
            click.secho(
                f"[Info ]update {_key} from {old_value} to {_value}", fg="green"
            )
            nonlocal _update
            _update = True

    if ip is not None:
        _update_config(config, "ip", ip)
    if port is not None:
        _update_config(config, "port", port)

    def _validate_ip(_config):
        if (_ip := _config.get("ip")) is None:
            click.secho(f"[Error]ip not set", fg="red")
            ctx.exit(100)
        else:
            try:
                import ipaddress

                ipaddress.IPv4Address(_ip)
                click.secho(f"[Info ]ip format {_ip} is valid", fg="green")
            except Exception:
                click.secho(f"[Error]ip format: {_ip} is invalid", fg="red")
                ctx.exit(100)

    _validate_ip(config)

    def _validate_port(_config):
        if (_port := _config.get("port")) is None:
            click.secho(f"[Error]port not set", fg="red")
            ctx.exit(100)
        else:
            try:
                _port = int(_port)
                if not (0 < _port < 65535):
                    raise ValueError
                click.secho(f"[Info ]port format: {_port} is valid", fg="green")
            except Exception:
                click.secho(f"[Error]port format: {_port} is invalid", fg="red")
                ctx.exit(100)

    _validate_port(config)

    if _update:
        with open(_get_config_path(), "w") as fout:
            yaml.dump(config, fout, Dumper=yaml.RoundTripDumper)
        click.secho(f"[Info ]config file has been successfully updated", fg="green")
    else:
        click.secho(f"[Info ]config file has not been changed", fg="green")


def _get_config_path():
    return os.path.join(os.path.dirname(__file__), "settings.yaml")


eggroll_cli.add_command(task.task)


if __name__ == "__main__":
    print(f"--start--")
    eggroll_cli()
