"""Python bridge for MATLAB-SciStack integration.

MATLAB calls ``py.scimatlab.bridge.<name>``; every such name is re-exported
here, from the module that owns it:

* :mod:`.entry` — the calls themselves: the two-pass ``for_each``
  (``for_each_prepare`` -> MATLAB's loop -> ``for_each_save``), batch save
  and load, entities, the PathInput project root, the batch cache;
* :mod:`.wire` — MATLAB specs rebuilt as Python wrappers, the function
  sentinel and :class:`MatlabFunctionIdentity`, variable surrogates;
* :mod:`.convert` — value conversion (buffers, datetimes, nested values);
* :mod:`.crossing` — datetime/timedelta scalars cross only boxed
  (MATLAB R2024a converts them bare);
* :mod:`.pipeline` — the Pipeline registry by handle.

``scimatlab/tests/test_bridge_entry_points.py`` holds this list to every
``py.scimatlab.bridge.<name>`` the MATLAB sources call.
"""

from .convert import (
    boxed_tolist,
    convert_nested_dicts_arrays_to_lists,
    convert_nested_dicts_to_json,
    datetimes_from_matlab,
    datetimes_to_matlab,
    durations_to_matlab,
    flatten_sequences,
    is_dataframe,
    is_datetime_value,
    matlab_zone_label,
    ndarray_to_buffer,
    numpy_scalar_to_python,
    split_df_to_dataframes,
    split_flat_to_lists,
)
from .crossing import (
    datetime_key_text_of,
    datetime_keys_as_text,
    DatetimeCrossing,
    DurationCrossing,
    box_datetimes,
    matlab_entry,
    unbox_datetimes,
)
from .entry import (
    _for_each_state_cache,
    codec_storage_columns,
    ensure_variable_classdefs,
    for_each_describe_loaded_input,
    for_each_prepare,
    for_each_save,
    free_batch,
    get_batch_data_item,
    get_batch_item,
    load_and_extract,
    load_entities,
    normalize_stat_result,
    pathinput_project_root,
    run_failure_group_json,
    save_batch_bridge,
    save_variable_bridge,
    schema_key_filter,
    set_pathinput_project_root,
    to_csv_bridge,
    variable_data,
    variable_filter,
    wrap_batch_bridge,
)
from .pipeline import (
    pipeline_active_name,
    pipeline_bind,
    pipeline_create,
    pipeline_deactivate,
    pipeline_endpoints,
    pipeline_execution_order,
    pipeline_plan,
    pipeline_register_step,
    pipeline_run_free,
    pipeline_run_python_step,
    pipeline_use,
)
from .wire import (
    MatlabFunctionIdentity,
    _locations_from_matlab,
    _make_matlab_fn_sentinel,
    get_surrogate_class,
    register_matlab_variable,
    sync_output_column_name,
)
