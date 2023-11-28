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
import yaml

from client.cli.commands import task
from client.cli.utils.cli_utils import prettify
from client.sdk import EggrollClient

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(short_help='Eggroll Client', context_settings=CONTEXT_SETTINGS)
@click.pass_context
def eggroll_cli(ctx):
    '''
    Eggroll Client
    '''
    ctx.ensure_object(dict)
    if ctx.invoked_subcommand == 'init':
        return

    with open(os.path.join(os.path.dirname(__file__), 'settings.yaml'), 'r') as fin:
        config = yaml.safe_load(fin)

    if config.get('ip') and config.get('port'):
        ctx.obj['ip'] = config['ip']
        ctx.obj['port'] = int(config['port'])
    else:
        raise ValueError('Invalid configuration file. Did you run "flow init"?')

    ctx.obj['initialized'] = (config.get('ip') and config.get('port'))
    if ctx.obj['initialized']:
        ctx.obj["client"] = EggrollClient(
            ip=config.get('ip'), port=config.get('port')
        )


@eggroll_cli.command('init', short_help='Eggroll CLI Init Command')
@click.option('--ip', type=click.STRING, help='Eggroll gip address.')
@click.option('--port', type=click.INT, help='Eggroll grpc port.')
def initialization(**kwargs):
    with open(os.path.join(os.path.dirname(__file__), 'settings.yaml'), 'r') as fin:
        config = yaml.safe_load(fin)

    if kwargs.get('reset'):
        for i in ('ip', 'port', 'app_id', 'app_token'):
            config[i] = None

        with open(os.path.join(os.path.dirname(__file__), 'settings.yaml'), 'w') as fout:
            yaml.dump(config, fout, Dumper=yaml.RoundTripDumper)
        prettify(
            {
                'retcode': 0,
                'retmsg': 'Eggroll CLI has been reset successfully'
            }
        )
    else:
        _update = False
        if not config:
            config = {}
        for i in ('ip', 'port'):
            if kwargs.get(i):
                config[i] = kwargs[i]
                _update = True

        if _update:
            with open(os.path.join(os.path.dirname(__file__), 'settings.yaml'), 'w') as fout:
                # yaml.dump(config, fout, Dumper=yaml.RoundTripDumper)
                yaml.dump(config, fout)
            prettify(
                {
                    'retcode': 0,
                    'retmsg': 'Eggroll CLI has been initialized successfully.'
                }
            )
        else:
            prettify(
                {
                    'retcode': 100,
                    'retmsg': 'Eggroll CLI initialization failed.'
                }
            )


eggroll_cli.add_command(task.task)


if __name__ == '__main__':
    print(f'--start--')
    eggroll_cli()
