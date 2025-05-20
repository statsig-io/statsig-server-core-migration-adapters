import json
from typing import Optional

import statsig.statsig_server as statsig_server
from statsig import globals, statsig
from statsig.dynamic_config import DynamicConfig
from statsig.evaluation_details import EvaluationDetails, DataSource, EvaluationReason
from statsig.feature_gate import FeatureGate
from statsig.initialize_details import InitializeDetails
from statsig.layer import Layer
from statsig.spec_store import _SpecStore
from statsig.statsig_error_boundary import _StatsigErrorBoundary
from statsig.statsig_event import StatsigEvent
from statsig.statsig_options import StatsigOptions
from statsig.statsig_user import StatsigUser
from statsig.utils import HashingAlgorithm
from statsig_python_core import DynamicConfigEvaluationOptions
from statsig_python_core import EvaluationDetails as RustEvaluationDetails
from statsig_python_core import FeatureGateEvaluationOptions, LayerEvaluationOptions, ExperimentEvaluationOptions
from statsig_python_core import InitializeDetails as RustInitializeDetails
from statsig_python_core import Statsig as RustStatsig
from statsig_python_core import StatsigOptions as RustStatsigOptions
from statsig_python_core import StatsigUser as RustStatsigUser
from typing_extensions import override

from secrets import PRODUCTION_SDK_KEY

DEFAULT_SPECS_URL = "https://api.statsigcdn.com/v2/download_config_specs"
DEFAULT_ID_LISTS_URL = "https://statsigapi.net/v1/get_id_lists"
DEFAULT_LOG_EVENT_URL = "https://prodregistryv2.org/v1/log_event"

EVAL_SOURCE_MAP = {
    "Adapter": "DataAdapter",
    "Uninitialized": "Uninitialized",
    "NoValues": "Uninitialized",
    "Error": "Uninitialized",
    "Loading": "Uninitialized",
    "Network": "Network",
    "Bootstrap": "Bootstrap",
    "Unsupported": "Unsupported",
}

EVAL_REASON_MAP = {
    "Recognized": "none",
    "Unrecognized": "Unrecognized",
    "LocalOverride": "LocalOverride",
    "Unsupported": "unsupported",
    "Error": "error",
}

def safe_enum(enum_cls, value):
    try:
        return enum_cls(value)
    except ValueError:
        return value


def convert_evaluation_details(details: RustEvaluationDetails) -> EvaluationDetails:
    parts = details.reason.split(":")
    source_str = parts[0] if len(parts) > 0 else None
    reason_str = parts[1] if len(parts) > 1 else None

    if source_str in ["NoValues", "Error", "Loading"]:
        reason_str = "Unrecognized"
    
    if source_str.startswith("Adapter"):
        source_str = "Adapter"

    mapped_source = (
        EVAL_SOURCE_MAP.get(source_str)
        or EVAL_REASON_MAP.get(source_str)
        or source_str
    )

    mapped_reason = (
        EVAL_REASON_MAP.get(reason_str)
        or EVAL_SOURCE_MAP.get(reason_str)
        or reason_str
    )

    return EvaluationDetails(
        config_sync_time=details.received_at,
        init_time=details.lcut,
        source=safe_enum(DataSource, mapped_source),
        reason=safe_enum(EvaluationReason, mapped_reason),
    )

def convert_initialize_details(init_details: RustInitializeDetails) -> InitializeDetails:
    return InitializeDetails(
        init_success=init_details.init_success,
        store_populated=init_details.is_config_spec_ready,
        duration=int(init_details.duration),
        source=init_details.source,
        error=init_details.failure_details.error if init_details.failure_details else None,
        timed_out=("timed out" in str(init_details.failure_details.error)) if init_details.failure_details else False,
    )

def configure_endpoints(endpoint: str, api: Optional[str], api_for_endpoint: Optional[str]) -> str:
    if endpoint == "download_config_specs":
        return api_for_endpoint.replace("v1", "v2") if api_for_endpoint else api.replace("v1", "v2") + "/download_config_specs" if api else DEFAULT_SPECS_URL
    elif endpoint == "get_id_lists":
        return api_for_endpoint or api + "/get_id_lists" if api else DEFAULT_ID_LISTS_URL
    elif endpoint == "log_event":
        return api_for_endpoint or api + "/log_event" if api else DEFAULT_LOG_EVENT_URL
    return ""


class StatsigServerAdapter(statsig_server.StatsigServer):
    _initialized: bool
    _init_details: RustInitializeDetails

    _options: StatsigOptions
    _spec_store: _SpecStore
    _sdk: RustStatsig

    def __init__(self) -> None:
        self._initialized = False

        self._errorBoundary = _StatsigErrorBoundary()

    @override
    def _initialize_impl(self, sdk_key: str, options: Optional[StatsigOptions]) -> InitializeDetails:
        options = options or StatsigOptions()
        if options.evaluation_callback is not None:
            raise NotImplementedError("Evaluation callback not implemented")

        self._sdk = RustStatsig(sdk_key=sdk_key, options=RustStatsigOptions(
            specs_url=configure_endpoints("download_config_specs", options.api, options.api_for_download_config_specs),
            specs_sync_interval_ms=(
                int(options.rulesets_sync_interval * 1000)
                if options.rulesets_sync_interval
                else None
            ),
            init_timeout_ms=(
                int(options.init_timeout * 1000) if options.init_timeout else None
            ),
            log_event_url=configure_endpoints("log_event", options.api, options.api_for_log_event),
            disable_all_logging=options.disable_all_logging,
            disable_network=options.local_mode,
            event_logging_flush_interval_ms=None,  # deprecated
            event_logging_max_queue_size=options.event_queue_size,
            event_logging_max_pending_batch_queue_size=options.retry_queue_size,
            enable_id_lists=True,
            wait_for_user_agent_init=True,  # UA slows down init, default lazy load in background
            wait_for_country_lookup_init=True,  # Country lookup slows down init, default lazy load in background
            disable_user_agent_parsing=None,
            disable_country_lookup=None,
            id_lists_url=configure_endpoints("get_id_lists", options.api, options.api_for_get_id_lists),
            id_lists_sync_interval_ms=(
                int(options.idlists_sync_interval * 1000)
                if options.idlists_sync_interval
                else None
            ),
            fallback_to_statsig_api=options.fallback_to_statsig_api,
            environment=options._environment["tier"] if options._environment else None,
            output_log_level="Debug",  # Warn/Error/Info/Debug
            global_custom_fields=None,  # -> This is where we put the infra selectors <-
            observability_client=options.observability_client,
            # -> The compat is not perfect here, if we need it we will have to create an adapter class <-
            data_store=options.data_store,  # for bootstrapping, you would need to set up a data store
            persistent_storage=None,
        ))
        init_details_py = super()._initialize_impl(sdk_key, options)
        init_details = self._sdk.initialize_with_details().result()
        self._init_details = init_details  # recommend using init_details to debug
        return convert_initialize_details(init_details)

    def _initialize_instance(self):
        # this function is used to initialize certain components if timeout is hit. So not applicable to python core (handled by rust)
        # feel free to remove
        super()._initialize_instance()

    def is_initialized(self):
        if hasattr(self, "_init_details") and self._init_details:
            return self._init_details.init_success  # TODO: might need to reset this in shutdown in core
        else:
            return super().is_initialized()

    def is_store_populated(self):
        if hasattr(self, "_init_details") and self._init_details:
            return self._init_details.is_config_spec_ready
        else:
            return super().is_store_populated()

    def get_init_source(self):
        return "rust"

    def get_feature_gate(self, user: StatsigUser, gate_name: str, log_exposure=True):
        def task():
            rust_user = self.__normalize_user(user)
            result = self._sdk.get_feature_gate(rust_user, gate_name, FeatureGateEvaluationOptions(
                disable_exposure_logging=not log_exposure))
            feature_gate = FeatureGate(
                data=result.value,
                name=result.name,
                rule=result.rule_id,
                id_type=result.id_type,
                evaluation_details=convert_evaluation_details(result.details),
            )
            return feature_gate

        return self._errorBoundary.capture(
            "get_feature_gate", task, lambda: False, {"configName": gate_name}
        )

    def manually_log_gate_exposure(self, user: StatsigUser, gate_name: str):
        user = self.__normalize_user(user)
        self._sdk.manually_log_gate_exposure(user, gate_name)

    def get_config(self, user: StatsigUser, config_name: str, log_exposure=True):
        def task():
            rust_user = self.__normalize_user(user)
            result = self._sdk.get_dynamic_config(rust_user, config_name, DynamicConfigEvaluationOptions(
                disable_exposure_logging=not log_exposure))

            config = DynamicConfig(
                data=result.value,  # TODO: check if this is json
                name=result.name,
                rule=result.rule_id,
                evaluation_details=convert_evaluation_details(result.details),
            )
            return config

        return self._errorBoundary.capture(
            "get_config",
            task,
            lambda: DynamicConfig({}, config_name, ""),
            {"configName": config_name},
        )

    def manually_log_config_exposure(self, user: StatsigUser, config_name: str):
        user = self.__normalize_user(user)
        self._sdk.manually_log_dynamic_config_exposure(user, config_name)

    def log_exposure_for_config(self, config_eval: DynamicConfig):
        raise NotImplementedError("Not implemented")

    def get_experiment(
            self, user: StatsigUser, experiment_name: str, log_exposure=True
    ):
        def task():
            rust_user = self.__normalize_user(user)
            result = self._sdk.get_experiment(rust_user, experiment_name, ExperimentEvaluationOptions(
                disable_exposure_logging=not log_exposure,
            ))
            config = DynamicConfig(
                data=result.value,  # TODO: check if this is json
                name=result.name,
                rule=result.rule_id,
                user=user,
                group_name=result.group_name,
                evaluation_details=convert_evaluation_details(result.details),
                # we don't expose the rest of these fields, they are logged by rust
                secondary_exposures=None,
                passed_rule=None,
                version=None,
            )
            return config

        return self._errorBoundary.capture(
            "get_experiment",
            task,
            lambda: DynamicConfig({}, experiment_name, ""),
            {"configName": experiment_name},
        )

    def manually_log_experiment_exposure(self, user: StatsigUser, experiment_name: str):
        user = self.__normalize_user(user)
        self._sdk.manually_log_experiment_exposure(user, experiment_name)

    def get_layer(self, user: StatsigUser, layer_name: str, log_exposure=True) -> Layer:
        def task():
            rust_user = self.__normalize_user(user)
            result = self._sdk.get_layer(rust_user, layer_name, options=LayerEvaluationOptions(
                disable_exposure_logging=not log_exposure,
            ))

            layer = Layer._create(
                name=layer_name,
                value=result.value,
                rule=result.rule_id,
                group_name=result.group_name,
                allocated_experiment=result.allocated_experiment_name,
                evaluation_details=convert_evaluation_details(result.details),
            )
            return layer

        return self._errorBoundary.capture(
            "get_layer",
            task,
            lambda: Layer._create(layer_name, {}, ""),
            {"configName": layer_name},
        )

    def manually_log_layer_parameter_exposure(
            self, user: StatsigUser, layer_name: str, parameter_name: str
    ):
        user = self.__normalize_user(user)
        self._sdk.manually_log_layer_parameter_exposure(user, layer_name, parameter_name)

    def log_event(self, event: StatsigEvent):
        def task():
            event.user = self.__normalize_user(event.user)
            self._sdk.log_event(event.user, event.event_name, event.value, event.metadata)

        self._errorBoundary.swallow("log_event", task)

    def flush(self):
        self._sdk.flush_events()

    def shutdown(self):
        def task():
            self._errorBoundary.shutdown()
            self._sdk.shutdown().wait()
            globals.logger.shutdown()
            self._initialized = False
            self._init_details = None

        self._errorBoundary.swallow("shutdown", task)

    def override_gate(self, gate: str, value: bool, user_id: Optional[str] = None):
        self._sdk.override_gate(gate, value, user_id)

    def override_config(
            self, config: str, value: object, user_id: Optional[str] = None
    ):
        value = value.__dict__
        self._sdk.override_dynamic_config(config, value, user_id)

    def override_experiment(
            self, experiment: str, value: object, user_id: Optional[str] = None
    ):
        value = value.__dict__
        self._sdk.override_experiment(experiment, value, user_id)

    def override_layer(self, layer: str, value: object, user_id: Optional[str] = None):
        value = value.__dict__
        self._sdk.override_layer(layer, value, user_id)

    def remove_gate_override(self, gate: str, user_id: Optional[str] = None):
        self._sdk.remove_gate_override(gate, user_id)

    def remove_config_override(self, config: str, user_id: Optional[str] = None):
        self._sdk.remove_dynamic_config_override(config, user_id)

    def remove_experiment_override(
            self, experiment: str, user_id: Optional[str] = None
    ):
        self._sdk.remove_experiment_override(experiment, user_id)

    def remove_layer_override(self, layer: str, user_id: Optional[str] = None):
        self._sdk.remove_layer_override(layer, user_id)

    def remove_all_overrides(self):
        self._sdk.remove_all_overrides()

    def get_client_initialize_response(
            self, user: StatsigUser,
            client_sdk_key: Optional[str] = None,
            hash: Optional[HashingAlgorithm] = HashingAlgorithm.SHA256,
            include_local_overrides: Optional[bool] = False,
    ):
        rust_user = self.__normalize_user(user)
        hash_str = hash.value if hash is not None else None
        gcir_str = self._sdk.get_client_initialize_response(rust_user, hash_str, client_sdk_key,
                                                            include_local_overrides)

        return json.loads(gcir_str) if gcir_str else {}

    def evaluate_all(self, user: StatsigUser):
        # no longer supported in core sdks.
        raise NotImplementedError("Not implemented")

    def __normalize_user(self, user: StatsigUser) -> RustStatsigUser:
        rust_user = RustStatsigUser(
            user_id=user.user_id,
            email=user.email,
            ip=user.ip,
            country=user.country,
            locale=user.locale,
            app_version=user.app_version,
            user_agent=user.user_agent,
            custom=user.custom,
            custom_ids=user.custom_ids,
            private_attributes=user.private_attributes,
        )
        return rust_user


if __name__ == "__main__":
    statsig.__instance = StatsigServerAdapter()

    # statsig.initialize(os.environ["STATSIG_SERVER_KEY"])
    statsig.initialize(PRODUCTION_SDK_KEY)

    gate = statsig.get_feature_gate(user=StatsigUser(user_id="test"), gate="pass")
    print(gate.value)
    print(gate.evaluation_details)
    config = statsig.get_config(user=StatsigUser(user_id="test"), config="cool_config")
    print(config.value)
    print(config.evaluation_details.source)

    gcir = statsig.get_client_initialize_response(StatsigUser(user_id="test"), client_sdk_key=PRODUCTION_SDK_KEY,
                                                  hash=HashingAlgorithm.NONE)

    statsig.shutdown()
