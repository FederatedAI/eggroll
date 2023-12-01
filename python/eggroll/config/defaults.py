from dataclasses import dataclass
from omegaconf import MISSING, DictConfig


@dataclass
class DefaultConfig:
    @dataclass
    class EggrollConfig:
        @dataclass
        class GCConfig:
            disabled: bool = False

        @dataclass
        class ResourcemanagerConfig:
            @dataclass
            class ClustermanagerConfig:
                @dataclass
                class JdbcConfig:
                    driver: DictConfig = DictConfig({"class": {"name": MISSING}})
                    url: str = MISSING
                    username: str = MISSING
                    password: str = MISSING

                host: str = MISSING
                port: int = MISSING
                jdbc: JdbcConfig = JdbcConfig()

            @dataclass
            class NodemanagerConfig:
                @dataclass
                class ContainersConfig:
                    @dataclass
                    class DataConfig:
                        dir: str = MISSING

                    data: DataConfig = DataConfig()

                host: str = MISSING
                port: int = MISSING
                containers: ContainersConfig = ContainersConfig()

            @dataclass
            class ProcessConfig:
                tag: str = MISSING

            @dataclass
            class BootstrapConfig:
                @dataclass
                class EggPairConfig:
                    exepath: str = MISSING
                    venv: str = MISSING
                    pythonpath: str = MISSING
                    filepath: str = MISSING
                    ld_library_path: str = MISSING

                egg_pair: EggPairConfig = EggPairConfig()

            clustermanager: ClustermanagerConfig = ClustermanagerConfig()
            nodemanager: NodemanagerConfig = NodemanagerConfig()
            process: ProcessConfig = ProcessConfig()
            bootstrap: BootstrapConfig = BootstrapConfig()

        @dataclass
        class SessionConfig:
            @dataclass
            class ProcessorsConfig:
                @dataclass
                class PerConfig:
                    node: int = MISSING

                per: PerConfig = PerConfig()

            @dataclass
            class StartConfig:
                @dataclass
                class TimeoutConfig:
                    ms: int = 20000

                timeout: TimeoutConfig = TimeoutConfig()

            processors: ProcessorsConfig = ProcessorsConfig()
            start: StartConfig = StartConfig()

        @dataclass
        class RollPairConfig:
            @dataclass
            class DefaultConfig:
                @dataclass
                class StoreConfig:
                    type: str = "ROLLPAIR_LMDB"

                store: StoreConfig = StoreConfig()

            @dataclass
            class DataConfig:
                @dataclass
                class ServerConfig:
                    @dataclass
                    class ExecutorConfig:
                        @dataclass
                        class PoolConfig:
                            @dataclass
                            class MaxConfig:
                                size: int = 5000

                            max: MaxConfig = MaxConfig()

                        pool: PoolConfig = PoolConfig()

                    executor: ExecutorConfig = ExecutorConfig()

                server: ServerConfig = ServerConfig()

            @dataclass
            class TransferPairConfig:
                @dataclass
                class BatchbrokerConfig:
                    @dataclass
                    class DefaultConfig:
                        size: int = 100

                    default: DefaultConfig = DefaultConfig()

                @dataclass
                class ExecutorConfig:
                    @dataclass
                    class PoolConfig:
                        @dataclass
                        class MaxConfig:
                            size: int = 5000

                        max: MaxConfig = MaxConfig()

                    pool: PoolConfig = PoolConfig()

                @dataclass
                class SendbufConfig:
                    size: int = MISSING

                sendbuf: SendbufConfig = SendbufConfig()
                executor: ExecutorConfig = ExecutorConfig()
                batchbroker: BatchbrokerConfig = BatchbrokerConfig()

            @dataclass
            class EggPairConfig:
                @dataclass
                class ServerConfig:
                    @dataclass
                    class HeartbeatConfig:
                        interval: int = 10

                    @dataclass
                    class ExecutorConfig:
                        @dataclass
                        class PoolConfig:
                            @dataclass
                            class MaxConfig:
                                size: int = 5000

                            max: MaxConfig = MaxConfig()

                        pool: PoolConfig = PoolConfig()

                    executor: ExecutorConfig = ExecutorConfig()
                    heartbeat: HeartbeatConfig = HeartbeatConfig()

                server: ServerConfig = ServerConfig()

            ransferpair: TransferPairConfig = TransferPairConfig()
            eggpair: EggPairConfig = EggPairConfig()
            data: DataConfig = DataConfig()
            default: DefaultConfig = DefaultConfig()
            transferpair: TransferPairConfig = TransferPairConfig()

        @dataclass
        class RollSiteConfig:
            @dataclass
            class PartyConfig:
                id: int = MISSING

            @dataclass
            class RouteConfig:
                @dataclass
                class TableConfig:
                    path: str = MISSING
                    key: str = MISSING
                    whitelist: str = MISSING

                table: TableConfig = TableConfig()

            @dataclass
            class JvmConfig:
                options: str = MISSING

            @dataclass
            class PushConfig:
                @dataclass
                class MaxConfig:
                    retry: int = MISSING

                @dataclass
                class LongConfig:
                    retry: int = MISSING

                @dataclass
                class BatchesConfig:
                    @dataclass
                    class PerStreamConfig:
                        stream: int = MISSING

                    per: PerStreamConfig = PerStreamConfig()

                max: MaxConfig = MaxConfig()
                long: LongConfig = LongConfig()
                batches: BatchesConfig = BatchesConfig()

            @dataclass
            class AdapterConfig:
                @dataclass
                class SendbufConfig:
                    size: int = MISSING

                sendbuf: SendbufConfig = SendbufConfig()

            coordinator: str = MISSING
            host: str = MISSING
            port: int = MISSING
            party: PartyConfig = PartyConfig()
            route: RouteConfig = RouteConfig()
            jvm: JvmConfig = JvmConfig()
            push: PushConfig = PushConfig()
            adapter: AdapterConfig = AdapterConfig()

        @dataclass
        class ZookeeperConfig:
            @dataclass
            class RegisterConfig:
                host: str = MISSING
                port: int = MISSING
                version: str = MISSING
                enable: bool = MISSING

            @dataclass
            class ServerConfig:
                host: str = MISSING
                port: int = MISSING

            register: RegisterConfig = RegisterConfig()
            server: ServerConfig = ServerConfig()

        @dataclass
        class JettyConfig:
            @dataclass
            class ServerConfig:
                port: int = MISSING

            server: ServerConfig = ServerConfig()

        @dataclass
        class SecurityConfig:
            @dataclass
            class LoginConfig:
                username: str = MISSING
                password: str = MISSING

            login: LoginConfig = LoginConfig()

        @dataclass
        class ContainerConfig:
            @dataclass
            class DeepspeedConfig:
                @dataclass
                class PythonConfig:
                    exec: str = MISSING

                @dataclass
                class DistributedConfig:
                    @dataclass
                    class StoreConfig:
                        host: str = MISSING
                        port: int = MISSING

                    backend: str = MISSING
                    store: StoreConfig = StoreConfig()

                @dataclass
                class ScriptConfig:
                    path: str = MISSING

                python: PythonConfig = PythonConfig()
                distributed: DistributedConfig = DistributedConfig()
                script: ScriptConfig = ScriptConfig()

            deepspeed: DeepspeedConfig = DeepspeedConfig()

        @dataclass
        class CoreConfig:
            # command.executor.pool.max.size
            @dataclass
            class ClientConfig:
                @dataclass
                class CommandConfig:
                    @dataclass
                    class ExecutorConfig:
                        @dataclass
                        class PoolConfig:
                            @dataclass
                            class MaxConfig:
                                size: int = 500

                            max: MaxConfig = MaxConfig()

                        pool: PoolConfig = PoolConfig()

                    executor: ExecutorConfig = ExecutorConfig()

                command: CommandConfig = CommandConfig()

            @dataclass
            class DefaultConfig:
                @dataclass
                class ExecutorConfig:
                    pool: str = "eggroll.core.datastructure.threadpool.ErThreadUnpooledExecutor"

                executor: ExecutorConfig = ExecutorConfig()

            @dataclass
            class GrpcConfig:
                @dataclass
                class ChannelConfig:
                    @dataclass
                    class SslConfig:
                        @dataclass
                        class SessionConfig:
                            @dataclass
                            class CacheConfig:
                                size: int = MISSING

                            @dataclass
                            class TimeoutConfig:
                                sec: int = MISSING

                            timeout: TimeoutConfig = TimeoutConfig()
                            cache: CacheConfig = CacheConfig()

                        session: SessionConfig = SessionConfig()

                    @dataclass
                    class PerConfig:
                        @dataclass
                        class RpcConfig:
                            @dataclass
                            class BufferConfig:
                                limit: int = 64 << 20

                            buffer: BufferConfig = BufferConfig()

                        rpc: RpcConfig = RpcConfig()

                    @dataclass
                    class Retry:
                        @dataclass
                        class BufferConfig:
                            size: int = 16 << 20

                        buffer: BufferConfig = BufferConfig()

                    @dataclass
                    class MaxConfig:
                        @dataclass
                        class InboundConfig:
                            @dataclass
                            class MessageConfig:
                                size: int = (2 << 30) - 1

                            @dataclass
                            class MetadataConfig:
                                size: int = 128 << 20

                            message: MessageConfig = MessageConfig()
                            metadata: MetadataConfig = MetadataConfig()

                        @dataclass
                        class RetryConfig:
                            attempts: int = 20

                        retry: RetryConfig = RetryConfig()
                        inbound: InboundConfig = InboundConfig()

                    @dataclass
                    class KeepaliveConfig:
                        @dataclass
                        class PermitConfig:
                            @dataclass
                            class WithoutConfig:
                                @dataclass
                                class CallsConfig:
                                    enabled: bool = False

                                calls: CallsConfig = CallsConfig()

                            without: WithoutConfig = WithoutConfig()

                        @dataclass
                        class TimeConfig:
                            sec: int = 7200

                        @dataclass
                        class TimeoutConfig:
                            sec: int = 3600

                        timeout: TimeoutConfig = TimeoutConfig()
                        time: TimeConfig = TimeConfig()
                        permit: PermitConfig = PermitConfig()

                    max: MaxConfig = MaxConfig()
                    per: PerConfig = PerConfig()
                    keepalive: KeepaliveConfig = KeepaliveConfig()
                    retry: Retry = Retry()
                    ssl: SslConfig = SslConfig()
                    terminate = DictConfig({"await": {"timeout": {"sec": MISSING}}})

                @dataclass
                class ServerConfig:
                    @dataclass
                    class ChannelConfig:
                        @dataclass
                        class RetryConfig:
                            @dataclass
                            class BufferConfig:
                                size: int = 16 << 20

                            buffer: BufferConfig = BufferConfig()

                        @dataclass
                        class MaxConfig:
                            @dataclass
                            class InboundConfig:
                                @dataclass
                                class MetadataConfig:
                                    size: int = 128 << 20

                                @dataclass
                                class MessageConfig:
                                    size: int = (2 << 30) - 1

                                metadata: MetadataConfig = MetadataConfig()
                                message: MessageConfig = MessageConfig()

                            inbound: InboundConfig = InboundConfig()

                        max: MaxConfig = MaxConfig()
                        retry: RetryConfig = RetryConfig()

                    channel: ChannelConfig = ChannelConfig()

                server: ServerConfig = ServerConfig()
                channel: ChannelConfig = ChannelConfig()

            default: DefaultConfig = DefaultConfig()
            grpc: GrpcConfig = GrpcConfig()
            client: ClientConfig = ClientConfig()

        @dataclass
        class DataConfig:
            dir: str = MISSING

        @dataclass
        class LogsConfig:
            dir: str = MISSING

        @dataclass
        class BootstrapConfig:
            @dataclass
            class RootConfig:
                script: str = MISSING

            root: RootConfig = RootConfig()

        resourcemanager: ResourcemanagerConfig = ResourcemanagerConfig()
        session: SessionConfig = SessionConfig()
        rollpair: RollPairConfig = RollPairConfig()
        rollsite: RollSiteConfig = RollSiteConfig()
        zookeeper: ZookeeperConfig = ZookeeperConfig()
        jetty: JettyConfig = JettyConfig()
        security: SecurityConfig = SecurityConfig()
        container: ContainerConfig = ContainerConfig()
        core: CoreConfig = CoreConfig()
        data: DataConfig = DataConfig()
        logs: LogsConfig = LogsConfig()
        bootstrap: BootstrapConfig = BootstrapConfig()
        home: str = MISSING
        gc: GCConfig = GCConfig()

    eggroll: EggrollConfig = EggrollConfig()
