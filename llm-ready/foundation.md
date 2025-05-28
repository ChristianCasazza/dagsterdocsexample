Today we are working with Dagster, the data engineering python library. We are going to build our base of understanding about Dagster before processing into our use case.

First, analyze, understand, and explain the core foundations of Dagster from the docs below. Explain the asset centric approach and how to build pipelines with Dagster. Then, provide a basic poc of your own dagster pipeline 

<docs>
# Repository Content

## Directory Structure (Selected Files)

├── curated_dagster_docs/foundation/api_quick_index/assets.md
├── curated_dagster_docs/foundation/api_quick_index/definitions.md
├── curated_dagster_docs/foundation/api_quick_index/resources.md
├── curated_dagster_docs/foundation/concepts_overview/concepts.md
├── curated_dagster_docs/foundation/core/defining_assets/defining-assets-with-asset-dependencies.md
├── curated_dagster_docs/foundation/core/defining_assets/defining-assets.md
├── curated_dagster_docs/foundation/core/defining_assets/metadata-and-tags.md
├── curated_dagster_docs/foundation/core/installation_quickstart/installation.md
├── curated_dagster_docs/foundation/core/installation_quickstart/quickstart.md
├── curated_dagster_docs/foundation/core/io_managers_resources_basics/external-resources.md
├── curated_dagster_docs/foundation/core/io_managers_resources_basics/io-managers.md

## Files

### File: curated_dagster_docs/foundation/api_quick_index/assets.md
```markdown

On this page

An asset is an object in persistent storage, such as a table, file, or persisted machine learning model. An asset definition is a description, in code, of an asset that should exist and how to produce and update that asset.

## Asset definitions [​](https://docs.dagster.io/api/dagster/assets\#asset-definitions "Direct link to Asset definitions")

Refer to the [Asset definitions](https://docs.dagster.io/guides/build/assets/defining-assets) documentation for more information.

@dagster.asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/asset_decorator.py#L120)

Create a definition for how to compute an asset.

A software-defined asset is the combination of:

1. An asset key, e.g. the name of a table.
2. A function, which can be run to compute the contents of the asset.
3. A set of upstream assets that are provided as inputs to the function when computing the asset.
Unlike an op, whose dependencies are determined by the graph it lives inside, an asset knows
about the upstream assets it depends on. The upstream assets are inferred from the arguments
to the decorated function. The name of the argument designates the name of the upstream asset.

An asset has an op inside it to represent the function that computes it. The name of the op
will be the segments of the asset key, separated by double-underscores.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the asset. If not provided, defaults to the name of the decorated function. The asset’s name must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, the asset’s key is the concatenation of the key\_prefix and the asset’s name, which defaults to the name of the decorated function. Each item in key\_prefix must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.
- **ins** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetIn_](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn) _\]_ _\]_) – A dictionary that maps input names to information about the input.
- **deps** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_AssetDep_](https://docs.dagster.io/api/dagster/assets#dagster.AssetDep) _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _str_ _\]_ _\]_ _\]_) – The assets that are upstream dependencies, but do not correspond to a parameter of the decorated function. If the AssetsDefinition for a multi\_asset is provided, dependencies on all assets created by the multi\_asset will be created.
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema)) – The configuration schema for the asset’s underlying op. If set, Dagster will check that config provided for the op matches this schema and fail if it does not. If not set, Dagster will accept any config provided for the op.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of metadata entries for the asset.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Set of resource handles required by the op.\
- **io\_manager\_key** ( _Optional_ _\[_ _str_ _\]_) – The resource key of the IOManager used for storing the output of the op as an asset, and for loading it in downstream ops (default: “io\_manager”). Only one of io\_manager\_key and io\_manager\_def can be provided.\
- **io\_manager\_def** ( _Optional_ _\[_ _object_ _\]_) – beta (Beta) The IOManager used for storing the output of the op as an asset, and for loading it in downstream ops. Only one of io\_manager\_def and io\_manager\_key can be provided.\
- **dagster\_type** ( _Optional_ _\[_ [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType) _\]_) – Allows specifying type validation functions that will be executed on the output of the decorated function after it runs.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the asset.\
- **op\_tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary of tags for the op that computes the asset. Frameworks may expect and require certain metadata to be attached to a op. Values that are not strings will be json encoded and must meet the criteria that json.loads(json.dumps(value)) == value.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. If not provided, the name “default” is used.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – beta (Beta) A mapping of resource keys to resources. These resources will be initialized during execution, and can be accessed from the context within the body of the function.\
- **output\_required** ( _bool_) – Whether the decorated function will always materialize an asset. Defaults to True. If False, the function can conditionally not yield a result. If no result is yielded, no output will be materialized to storage and downstream assets will not be materialized. Note that for output\_required to work at all, you must use yield in your asset logic rather than return. return will not respect this setting and will always produce an asset materialization, even if None is returned.\
- **automation\_condition** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – A condition describing when Dagster should materialize this asset.\
- **backfill\_policy** ( [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy)) – beta (Beta) Configure Dagster to backfill this asset according to its BackfillPolicy.\
- **retry\_policy** ( _Optional_ _\[_ [_RetryPolicy_](https://docs.dagster.io/api/dagster/ops#dagster.RetryPolicy) _\]_) – The retry policy for the op that computes the asset.\
- **code\_version** ( _Optional_ _\[_ _str_ _\]_) – Version of the code that generates this asset. In general, versions should be set only for code that deterministically produces the same output when given the same inputs.\
- **check\_specs** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckSpec_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec) _\]_ _\]_) – Specs for asset checks that execute in the decorated function after materializing the asset.\
- **key** ( _Optional_ _\[_ _CoeercibleToAssetKey_ _\]_) – The key for this asset. If provided, cannot specify key\_prefix or name.\
- **owners** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A list of strings representing owners of the asset. Each string can be a user’s email address, or a team name prefixed with team:, e.g. team:finops.\
- **kinds** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – A list of strings representing the kinds of the asset. These will be made visible in the Dagster UI.\
- **pool** ( _Optional_ _\[_ _str_ _\]_) – A string that identifies the concurrency pool that governs this asset’s execution.\
- **non\_argument\_deps** ( _Optional_ _\[_ _Union_ _\[_ _Set_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Set_ _\[_ _str_ _\]_ _\]_ _\]_) – deprecated Deprecated, use deps instead. Set of asset keys that are upstream dependencies, but do not pass an input to the asset. Hidden parameter not exposed in the decorator signature, but passed in kwargs.\
\
Examples:\
\
```codeBlockLines_e6Vv\
@asset\
def my_upstream_asset() -> int:\
    return 5\
\
@asset\
def my_asset(my_upstream_asset: int) -> int:\
    return my_upstream_asset + 1\
\
should_materialize = True\
\
@asset(output_required=False)\
def conditional_asset():\
    if should_materialize:\
        yield Output(5)  # you must `yield`, not `return`, the result\
\
# Will also only materialize if `should_materialize` is `True`\
@asset\
def downstream_asset(conditional_asset):\
    return conditional_asset + 1\
\
```\
\
`class` dagster.MaterializeResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/result.py#L62)\
\
An object representing a successful materialization of an asset. These can be returned from\
@asset and @multi\_asset decorated functions to pass metadata or specify specific assets were\
materialized.\
\
Parameters:\
\
- **asset\_key** ( _Optional_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – Optional in @asset, required in @multi\_asset to discern which asset this refers to.\
- **metadata** ( _Optional_ _\[_ _RawMetadataMapping_ _\]_) – Metadata to record with the corresponding AssetMaterialization event.\
- **check\_results** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckResult_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult) _\]_ _\]_) – Check results to record with the corresponding AssetMaterialization event.\
- **data\_version** ( _Optional_ _\[_ _DataVersion_ _\]_) – The data version of the asset that was observed.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags to record with the corresponding AssetMaterialization event.\
\
`class` dagster.AssetSpec  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_spec.py#L105)\
\
Specifies the core attributes of an asset, except for the function that materializes or\
observes it.\
\
An asset spec plus any materialization or observation function for the asset constitutes an\
“asset definition”.\
\
Parameters:\
\
- **key** ( [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)) – The unique identifier for this asset.\
- **deps** ( _Optional_ _\[_ _AbstractSet_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – The asset keys for the upstream assets that materializing this asset depends on.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – Human-readable description of this asset.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of static metadata for this asset. For example, users can provide information about the database table this asset corresponds to.\
- **skippable** ( _bool_) – Whether this asset can be omitted during materialization, causing downstream dependencies to skip.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. If not provided, the name “default” is used.\
- **code\_version** ( _Optional_ _\[_ _str_ _\]_) – The version of the code for this specific asset, overriding the code version of the materialization function\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – BackfillPolicy to apply to the specified asset.\
- **owners** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A list of strings representing owners of the asset. Each string can be a user’s email address, or a team name prefixed with team:, e.g. team:finops.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
- **kinds** – (Optional\[Set\[str\]\]): A set of strings representing the kinds of the asset. These will be made visible in the Dagster UI.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the asset.\
\
merge\_attributes  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_spec.py#L357)\
\
Returns a new AssetSpec with the specified attributes merged with the current attributes.\
\
Parameters:\
\
- **deps** ( _Optional_ _\[_ _Iterable_ _\[_ _CoercibleToAssetDep_ _\]_ _\]_) – A set of asset dependencies to add to the asset self.\
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A set of metadata to add to the asset self. Will overwrite any existing metadata with the same key.\
- **owners** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A set of owners to add to the asset self.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of tags to add to the asset self. Will overwrite any existing tags with the same key.\
- **kinds** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – A set of kinds to add to the asset self.\
\
Returns: AssetSpec\
\
replace\_attributes  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_spec.py#L314)\
\
Returns a new AssetSpec with the specified attributes replaced.\
\
with\_io\_manager\_key  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_spec.py#L298)\
\
Returns a copy of this AssetSpec with an extra metadata value that dictates which I/O\
manager to use to load the contents of this asset in downstream computations.\
\
Parameters: **io\_manager\_key** ( _str_) – The I/O manager key. This will be used as the value for the\
“dagster/io\_manager\_key” metadata key.Returns: AssetSpec\
\
`class` dagster.AssetsDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L87)\
\
Defines a set of assets that are produced by the same op or graph.\
\
AssetsDefinitions are typically not instantiated directly, but rather produced using the\
[`@asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) or [`@multi_asset`](https://docs.dagster.io/api/dagster/assets#dagster.multi_asset) decorators.\
\
`static` from\_graph  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L414)\
\
Constructs an AssetsDefinition from a GraphDefinition.\
\
Parameters:\
\
- **graph\_def** ( [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition)) – The GraphDefinition that is an asset.\
- **keys\_by\_input\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – A mapping of the input names of the decorated graph to their corresponding asset keys. If not provided, the input asset keys will be created from the graph input names.\
- **keys\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – A mapping of the output names of the decorated graph to their corresponding asset keys. If not provided, the output asset keys will be created from the graph output names.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, key\_prefix will be prepended to each key in keys\_by\_output\_name. Each item in key\_prefix must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.\
- **internal\_asset\_deps** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Set_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_ _\]_) – By default, it is assumed that all assets produced by the graph depend on all assets that are consumed by that graph. If this default is not correct, you pass in a map of output names to a corrected set of AssetKeys that they depend on. Any AssetKeys in this list must be either used as input to the asset or produced within the graph.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the assets.\
- **partition\_mappings** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_PartitionMapping_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionMapping) _\]_ _\]_) – Defines how to map partition keys for this asset to partition keys of upstream assets. Each key in the dictionary correponds to one of the input assets, and each value is a PartitionMapping. If no entry is provided for a particular asset dependency, the partition mapping defaults to the default partition mapping for the partitions definition, which is typically maps partition keys to the same partition keys in upstream assets.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_ResourceDefinition_](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) _\]_ _\]_) – beta (Beta) A mapping of resource keys to resource definitions. These resources will be initialized during execution, and can be accessed from the body of ops in the graph during execution.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A group name for the constructed asset. Assets without a group name are assigned to a group called “default”.\
- **group\_names\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _str_ _\]_ _\]_ _\]_) – Defines a group name to be associated with some or all of the output assets for this node. Keys are names of the outputs, and values are the group name. Cannot be used with the group\_name argument.\
- **descriptions\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _str_ _\]_ _\]_ _\]_) – Defines a description to be associated with each of the output asstes for this graph.\
- **metadata\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _RawMetadataMapping_ _\]_ _\]_ _\]_) – Defines metadata to be associated with each of the output assets for this node. Keys are names of the outputs, and values are dictionaries of metadata to be associated with the related asset.\
- **tags\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_ _\]_ _\]_) – Defines tags to be associated with each of the output assets for this node. Keys are the names of outputs, and values are dictionaries of tags to be associated with the related asset.\
- **freshness\_policies\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _FreshnessPolicy_ _\]_ _\]_ _\]_) – Defines a FreshnessPolicy to be associated with some or all of the output assets for this node. Keys are the names of the outputs, and values are the FreshnessPolicies to be attached to the associated asset.\
- **automation\_conditions\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_ _\]_ _\]_) – Defines an AutomationCondition to be associated with some or all of the output assets for this node. Keys are the names of the outputs, and values are the AutoMaterializePolicies to be attached to the associated asset.\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – Defines this asset’s BackfillPolicy\
- **owners\_by\_key** ( _Optional_ _\[_ _Mapping_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – Defines owners to be associated with each of the asset keys for this node.\
\
`static` from\_op  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L531)\
\
Constructs an AssetsDefinition from an OpDefinition.\
\
Parameters:\
\
- **op\_def** ( [_OpDefinition_](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition)) – The OpDefinition that is an asset.\
- **keys\_by\_input\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – A mapping of the input names of the decorated op to their corresponding asset keys. If not provided, the input asset keys will be created from the op input names.\
- **keys\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – A mapping of the output names of the decorated op to their corresponding asset keys. If not provided, the output asset keys will be created from the op output names.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, key\_prefix will be prepended to each key in keys\_by\_output\_name. Each item in key\_prefix must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.\
- **internal\_asset\_deps** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Set_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_ _\]_) – By default, it is assumed that all assets produced by the op depend on all assets that are consumed by that op. If this default is not correct, you pass in a map of output names to a corrected set of AssetKeys that they depend on. Any AssetKeys in this list must be either used as input to the asset or produced within the op.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the assets.\
- **partition\_mappings** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_PartitionMapping_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionMapping) _\]_ _\]_) – Defines how to map partition keys for this asset to partition keys of upstream assets. Each key in the dictionary correponds to one of the input assets, and each value is a PartitionMapping. If no entry is provided for a particular asset dependency, the partition mapping defaults to the default partition mapping for the partitions definition, which is typically maps partition keys to the same partition keys in upstream assets.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A group name for the constructed asset. Assets without a group name are assigned to a group called “default”.\
- **group\_names\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _str_ _\]_ _\]_ _\]_) – Defines a group name to be associated with some or all of the output assets for this node. Keys are names of the outputs, and values are the group name. Cannot be used with the group\_name argument.\
- **descriptions\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _str_ _\]_ _\]_ _\]_) – Defines a description to be associated with each of the output asstes for this graph.\
- **metadata\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _RawMetadataMapping_ _\]_ _\]_ _\]_) – Defines metadata to be associated with each of the output assets for this node. Keys are names of the outputs, and values are dictionaries of metadata to be associated with the related asset.\
- **tags\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_ _\]_ _\]_) – Defines tags to be associated with each othe output assets for this node. Keys are the names of outputs, and values are dictionaries of tags to be associated with the related asset.\
- **freshness\_policies\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ _FreshnessPolicy_ _\]_ _\]_ _\]_) – Defines a FreshnessPolicy to be associated with some or all of the output assets for this node. Keys are the names of the outputs, and values are the FreshnessPolicies to be attached to the associated asset.\
- **automation\_conditions\_by\_output\_name** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_ _\]_ _\]_) – Defines an AutomationCondition to be associated with some or all of the output assets for this node. Keys are the names of the outputs, and values are the AutoMaterializePolicies to be attached to the associated asset.\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – Defines this asset’s BackfillPolicy\
\
get\_asset\_spec  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L1454)\
\
Returns a representation of this asset as an [`AssetSpec`](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec).\
\
If this is a multi-asset, the “key” argument allows selecting which asset to return the\
spec for.\
\
Parameters: **key** ( _Optional_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – If this is a multi-asset, select which asset to return its\
AssetSpec. If not a multi-asset, this can be left as None.Returns: AssetSpec\
\
get\_partition\_mapping  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L1089)\
\
Returns the partition mapping between keys in this AssetsDefinition and a given input\
asset key (if any).\
\
to\_source\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L1390)\
\
Returns a representation of this asset as a [`SourceAsset`](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset).\
\
If this is a multi-asset, the “key” argument allows selecting which asset to return a\
SourceAsset representation of.\
\
Parameters: **key** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_ _\]_) – If this is a multi-asset, select
which asset to return a SourceAsset representation of. If not a multi-asset, this
can be left as None.Returns: SourceAsset

to\_source\_assets  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/assets.py#L1378)

Returns a SourceAsset for each asset in this definition.

Each produced SourceAsset will have the same key, metadata, io\_manager\_key, etc. as the
corresponding asset

`property` asset\_deps

Maps assets that are produced by this definition to assets that they depend on. The
dependencies can be either “internal”, meaning that they refer to other assets that are
produced by this definition, or “external”, meaning that they refer to assets that aren’t
produced by this definition.

`property` can\_subset

If True, indicates that this AssetsDefinition may materialize any subset of its
asset keys in a given computation (as opposed to being required to materialize all asset
keys).

Type: bool

`property` check\_specs

Returns the asset check specs defined on this AssetsDefinition, i.e. the checks that can
be executed while materializing the assets.

Return type: Iterable\[AssetsCheckSpec\]

`property` dependency\_keys

The asset keys which are upstream of any asset included in this
AssetsDefinition.

Type: Iterable\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)\]

`property` descriptions\_by\_key

Returns a mapping from the asset keys in this AssetsDefinition
to the descriptions assigned to them. If there is no assigned description for a given AssetKey,
it will not be present in this dictionary.

Type: Mapping\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey), str\]

`property` group\_names\_by\_key

Returns a mapping from the asset keys in this AssetsDefinition
to the group names assigned to them. If there is no assigned group name for a given AssetKey,
it will not be present in this dictionary.

Type: Mapping\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey), str\]

`property` key

The asset key associated with this AssetsDefinition. If this AssetsDefinition
has more than one asset key, this will produce an error.

Type: [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)

`property` keys

The asset keys associated with this AssetsDefinition.

Type: AbstractSet\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)\]

`property` node\_def

Returns the OpDefinition or GraphDefinition that is used to materialize
the assets in this AssetsDefinition.

Type: NodeDefinition

`property` op

Returns the OpDefinition that is used to materialize the assets in this
AssetsDefinition.

Type: [OpDefinition](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition)

`property` partitions\_def

The PartitionsDefinition for this AssetsDefinition (if any).

Type: Optional\[ [PartitionsDefinition](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition)\]

`property` required\_resource\_keys

The set of keys for resources that must be provided to this AssetsDefinition.

Type: Set\[str\]

`property` resource\_defs

A mapping from resource name to ResourceDefinition for
the resources bound to this AssetsDefinition.

Type: Mapping\[str, [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]

`class` dagster.AssetKey  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_key.py#L28)

Object representing the structure of an asset key. Takes in a sanitized string, list of
strings, or tuple of strings.

Example usage:

```codeBlockLines_e6Vv
from dagster import AssetKey

AssetKey("asset1")
AssetKey(["asset1"]) # same as the above
AssetKey(["prefix", "asset1"])
AssetKey(["prefix", "subprefix", "asset1"])

```

Parameters: **path** ( _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_) – String, list of strings, or tuple of strings. A list of
strings represent the hierarchical structure of the asset\_key.

`property` pathdagster.map\_asset\_specs  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_spec.py#L423)

Map a function over a sequence of AssetSpecs or AssetsDefinitions, replacing specs in the sequence
or specs in an AssetsDefinitions with the result of the function.

Parameters:

- **func** ( _Callable_ _\[_ _\[_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_) – The function to apply to each AssetSpec.
- **iterable** ( _Iterable_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _\]_) – The sequence of AssetSpecs or AssetsDefinitions.

Returns:
A sequence of AssetSpecs or AssetsDefinitions with the function applied
to each spec.

Return type: Sequence\[Union\[ [AssetsDefinition](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [AssetSpec](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec)\]\]
Examples:

```codeBlockLines_e6Vv
from dagster import AssetSpec, map_asset_specs

asset_specs = [\
    AssetSpec(key="my_asset"),\
    AssetSpec(key="my_asset_2"),\
]

mapped_specs = map_asset_specs(lambda spec: spec.replace_attributes(owners=["nelson@hooli.com"]), asset_specs)

```

## Graph-backed asset definitions [​](https://docs.dagster.io/api/dagster/assets\#graph-backed-asset-definitions "Direct link to Graph-backed asset definitions")

Refer to the [Graph-backed asset](https://docs.dagster.io/guides/build/assets/defining-assets#graph-asset) documentation for more information.

@dagster.graph\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/asset_decorator.py#L774)

Creates a software-defined asset that’s computed using a graph of ops.

This decorator is meant to decorate a function that composes a set of ops or graphs to define
the dependencies between them.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the asset. If not provided, defaults to the name of the decorated function. The asset’s name must be a valid name in Dagster (ie only contains letters, numbers, and underscores) and may not contain Python reserved keywords.

- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the asset.

- **ins** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetIn_](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn) _\]_ _\]_) – A dictionary that maps input names to information about the input.

- **config** ( _Optional_ _\[_ _Union_ _\[_ [_ConfigMapping_](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) _\]_ _,_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) –\
\
Describes how the graph underlying the asset is configured at runtime.\
\
If a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) object is provided, then the graph takes on the config schema of this object. The mapping will be applied at runtime to generate the config for the graph’s constituent nodes.\
\
If a dictionary is provided, then it will be used as the default run config for the graph. This means it must conform to the config schema of the underlying nodes. Note that the values provided will be viewable and editable in the Dagster UI, so be careful with secrets.\
\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, the asset’s key is the concatenation of the key\_prefix and the asset’s name, which defaults to the name of the decorated function. Each item in key\_prefix must be a valid name in Dagster (ie only contains letters, numbers, and underscores) and may not contain Python reserved keywords.\
\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. If not provided, the name “default” is used.\
\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the asset.\
\
- **metadata** ( _Optional_ _\[_ _RawMetadataMapping_ _\]_) – Dictionary of metadata to be associated with the asset.\
\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
\
- **owners** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A list of strings representing owners of the asset. Each string can be a user’s email address, or a team name prefixed with team:, e.g. team:finops.\
\
- **kinds** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – A list of strings representing the kinds of the asset. These will be made visible in the Dagster UI.\
\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – The AutomationCondition to use for this asset.\
\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – The BackfillPolicy to use for this asset.\
\
- **code\_version** ( _Optional_ _\[_ _str_ _\]_) – Version of the code that generates this asset. In general, versions should be set only for code that deterministically produces the same output when given the same inputs.\
\
- **key** ( _Optional_ _\[_ _CoeercibleToAssetKey_ _\]_) – The key for this asset. If provided, cannot specify key\_prefix or name.\
\
\
Examples:\
\
```codeBlockLines_e6Vv\
@op\
def fetch_files_from_slack(context) -> pd.DataFrame:\
    ...\
\
@op\
def store_files(files) -> None:\
    files.to_sql(name="slack_files", con=create_db_connection())\
\
@graph_asset\
def slack_files_table():\
    return store_files(fetch_files_from_slack())\
\
```\
\
@dagster.graph\_multi\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/asset_decorator.py#L1013)\
\
Create a combined definition of multiple assets that are computed using the same graph of\
ops, and the same upstream assets.\
\
Each argument to the decorated function references an upstream asset that this asset depends on.\
The name of the argument designates the name of the upstream asset.\
\
Parameters:\
\
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the graph.\
\
- **outs** – (Optional\[Dict\[str, AssetOut\]\]): The AssetOuts representing the produced assets.\
\
- **ins** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetIn_](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn) _\]_ _\]_) – A dictionary that maps input names to information about the input.\
\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the assets.\
\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – The backfill policy for the asset.\
\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. This group name will be applied to all assets produced by this multi\_asset.\
\
- **can\_subset** ( _bool_) – Whether this asset’s computation can emit a subset of the asset keys based on the context.selected\_assets argument. Defaults to False.\
\
- **config** ( _Optional_ _\[_ _Union_ _\[_ [_ConfigMapping_](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) _\]_ _,_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) –\
\
Describes how the graph underlying the asset is configured at runtime.\
\
If a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) object is provided, then the graph takes on the config schema of this object. The mapping will be applied at runtime to generate the config for the graph’s constituent nodes.\
\
If a dictionary is provided, then it will be used as the default run config for the graph. This means it must conform to the config schema of the underlying nodes. Note that the values provided will be viewable and editable in the Dagster UI, so be careful with secrets.\
\
If no value is provided, then the config schema for the graph is the default (derived\
\
\
## Multi-asset definitions [​](https://docs.dagster.io/api/dagster/assets\#multi-asset-definitions "Direct link to Multi-asset definitions")\
\
Refer to the [Multi-asset](https://docs.dagster.io/guides/build/assets/defining-assets#multi-asset) documentation for more information.\
\
@dagster.multi\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/asset_decorator.py#L556)\
\
Create a combined definition of multiple assets that are computed using the same op and same\
upstream assets.\
\
Each argument to the decorated function references an upstream asset that this asset depends on.\
The name of the argument designates the name of the upstream asset.\
\
You can set I/O managers keys, auto-materialize policies, freshness policies, group names, etc.\
on an individual asset within the multi-asset by attaching them to the [`AssetOut`](https://docs.dagster.io/api/dagster/assets#dagster.AssetOut)\
corresponding to that asset in the outs parameter.\
\
Parameters:\
\
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the op.\
- **outs** – (Optional\[Dict\[str, AssetOut\]\]): The AssetOuts representing the assets materialized by this function. AssetOuts detail the output, IO management, and core asset properties. This argument is required except when AssetSpecs are used.\
- **ins** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_AssetIn_](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn) _\]_ _\]_) – A dictionary that maps input names to information about the input.\
- **deps** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _str_ _\]_ _\]_ _\]_) – The assets that are upstream dependencies, but do not correspond to a parameter of the decorated function. If the AssetsDefinition for a multi\_asset is provided, dependencies on all assets created by the multi\_asset will be created.\
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema)) – The configuration schema for the asset’s underlying op. If set, Dagster will check that config provided for the op matches this schema and fail if it does not. If not set, Dagster will accept any config provided for the op.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Set of resource handles required by the underlying op.\
- **internal\_asset\_deps** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Set_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_ _\]_) – By default, it is assumed that all assets produced by a multi\_asset depend on all assets that are consumed by that multi asset. If this default is not correct, you pass in a map of output names to a corrected set of AssetKeys that they depend on. Any AssetKeys in this list must be either used as input to the asset or produced within the op.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the assets.\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – The backfill policy for the op that computes the asset.\
- **op\_tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary of tags for the op that computes the asset. Frameworks may expect and require certain metadata to be attached to a op. Values that are not strings will be json encoded and must meet the criteria that json.loads(json.dumps(value)) == value.\
- **can\_subset** ( _bool_) – If this asset’s computation can emit a subset of the asset keys based on the context.selected\_asset\_keys argument. Defaults to False.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – beta (Beta) A mapping of resource keys to resources. These resources will be initialized during execution, and can be accessed from the context within the body of the function.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. This group name will be applied to all assets produced by this multi\_asset.\
- **retry\_policy** ( _Optional_ _\[_ [_RetryPolicy_](https://docs.dagster.io/api/dagster/ops#dagster.RetryPolicy) _\]_) – The retry policy for the op that computes the asset.\
- **code\_version** ( _Optional_ _\[_ _str_ _\]_) – Version of the code encapsulated by the multi-asset. If set, this is used as a default code version for all defined assets.\
- **specs** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _\]_) – The specifications for the assets materialized by this function.\
- **check\_specs** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckSpec_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec) _\]_ _\]_) – Specs for asset checks that execute in the decorated function after materializing the assets.\
- **pool** ( _Optional_ _\[_ _str_ _\]_) – A string that identifies the concurrency pool that governs this multi-asset’s execution.\
- **non\_argument\_deps** ( _Optional_ _\[_ _Union_ _\[_ _Set_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Set_ _\[_ _str_ _\]_ _\]_ _\]_) – deprecated Deprecated, use deps instead. Set of asset keys that are upstream dependencies, but do not pass an input to the multi\_asset.\
\
Examples:\
\
```codeBlockLines_e6Vv\
@multi_asset(\
    specs=[\
        AssetSpec("asset1", deps=["asset0"]),\
        AssetSpec("asset2", deps=["asset0"]),\
    ]\
)\
def my_function():\
    asset0_value = load(path="asset0")\
    asset1_result, asset2_result = do_some_transformation(asset0_value)\
    write(asset1_result, path="asset1")\
    write(asset2_result, path="asset2")\
\
# Or use IO managers to handle I/O:\
@multi_asset(\
    outs={\
        "asset1": AssetOut(),\
        "asset2": AssetOut(),\
    }\
)\
def my_function(asset0):\
    asset1_value = do_some_transformation(asset0)\
    asset2_value = do_some_other_transformation(asset0)\
    return asset1_value, asset2_value\
\
```\
\
@dagster.multi\_observable\_source\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/source_asset_decorator.py#L230)\
\
beta\
\
This API is currently in beta, and may have breaking changes in minor version releases, with behavior changes in patch releases.\
\
Defines a set of assets that can be observed together with the same function.\
\
Parameters:\
\
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the op.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Set of resource handles required by the underlying op.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the assets.\
- **can\_subset** ( _bool_) – If this asset’s computation can emit a subset of the asset keys based on the context.selected\_assets argument. Defaults to False.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – beta (Beta) A mapping of resource keys to resources. These resources will be initialized during execution, and can be accessed from the context within the body of the function.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. This group name will be applied to all assets produced by this multi\_asset.\
- **specs** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _\]_) – The specifications for the assets observed by this function.\
- **check\_specs** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckSpec_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckSpec) _\]_ _\]_) – Specs for asset checks that execute in the decorated function after observing the assets.\
\
Examples:\
\
```codeBlockLines_e6Vv\
@multi_observable_source_asset(\
    specs=[AssetSpec("asset1"), AssetSpec("asset2")],\
)\
def my_function():\
    yield ObserveResult(asset_key="asset1", metadata={"foo": "bar"})\
    yield ObserveResult(asset_key="asset2", metadata={"baz": "qux"})\
\
```\
\
`class` dagster.AssetOut  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_out.py#L34)\
\
Defines one of the assets produced by a [`@multi_asset`](https://docs.dagster.io/api/dagster/assets#dagster.multi_asset).\
\
Parameters:\
\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, the asset’s key is the concatenation of the key\_prefix and the asset’s name. When using `@multi_asset`, the asset name defaults to the key of the “outs” dictionary Only one of the “key\_prefix” and “key” arguments should be provided.\
- **key** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – The asset’s key. Only one of the “key\_prefix” and “key” arguments should be provided.\
- **dagster\_type** ( _Optional_ _\[_ _Union_ _\[_ _Type_ _,_ [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType) _\]_ _\]_ _\]_) – The type of this output. Should only be set if the correct type can not be inferred directly from the type signature of the decorated function.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – Human-readable description of the output.\
- **is\_required** ( _bool_) – Whether the presence of this field is required. (default: True)\
- **io\_manager\_key** ( _Optional_ _\[_ _str_ _\]_) – The resource key of the IO manager used for this output. (default: “io\_manager”).\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of the metadata for the output. For example, users can provide a file path if the data object will be stored in a filesystem, or provide information of a database table when it is going to load the data into the table.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. If not provided, the name “default” is used.\
- **code\_version** ( _Optional_ _\[_ _str_ _\]_) – The version of the code that generates this asset.\
- **freshness\_policy** ( _Optional_ _\[_ _FreshnessPolicy_ _\]_) – deprecated (Deprecated) A policy which indicates how up to date this asset is intended to be.\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – AutomationCondition to apply to the specified asset.\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – BackfillPolicy to apply to the specified asset.\
- **owners** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A list of strings representing owners of the asset. Each string can be a user’s email address, or a team name prefixed with team:, e.g. team:finops.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
- **kinds** ( _Optional_ _\[_ _set_ _\[_ _str_ _\]_ _\]_) – A set of strings representing the kinds of the asset. These\
\
will be made visible in the Dagster UI.\
\
`static` from\_spec  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_out.py#L237)\
\
Builds an AssetOut from the passed spec.\
\
Parameters:\
\
- **spec** ( [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec)) – The spec to build the AssetOut from.\
- **dagster\_type** ( _Optional_ _\[_ _Union_ _\[_ _Type_ _,_ [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType) _\]_ _\]_) – The type of this output. Should only be set if the correct type can not be inferred directly from the type signature of the decorated function.\
- **is\_required** ( _bool_) – Whether the presence of this field is required. (default: True)\
- **io\_manager\_key** ( _Optional_ _\[_ _str_ _\]_) – The resource key of the IO manager used for this output. (default: “io\_manager”).\
- **backfill\_policy** ( _Optional_ _\[_ [_BackfillPolicy_](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy) _\]_) – BackfillPolicy to apply to the specified asset.\
\
Returns: The AssetOut built from the spec.Return type: [AssetOut](https://docs.dagster.io/api/dagster/assets#dagster.AssetOut)\
\
## Source assets [​](https://docs.dagster.io/api/dagster/assets\#source-assets "Direct link to Source assets")\
\
Refer to the [External asset dependencies](https://docs.dagster.io/guides/build/assets/external-assets) documentation for more information.\
\
`class` dagster.SourceAsset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/source_asset.py#L161)\
\
deprecated\
\
This API will be removed in version 2.0.0.\
Use AssetSpec instead. If using the SourceAsset io\_manager\_key property, use AssetSpec(...).with\_io\_manager\_key(...)..\
\
A SourceAsset represents an asset that will be loaded by (but not updated by) Dagster.\
\
Parameters:\
\
- **key** ( _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _str_ _\]_) – The key of the asset.\
- **metadata** ( _Mapping_ _\[_ _str_ _,_ [_MetadataValue_](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) _\]_) – Metadata associated with the asset.\
- **io\_manager\_key** ( _Optional_ _\[_ _str_ _\]_) – The key for the IOManager that will be used to load the contents of the asset when it’s used as an input to other assets inside a job.\
- **io\_manager\_def** ( _Optional_ _\[_ [_IOManagerDefinition_](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) _\]_) – beta (Beta) The definition of the IOManager that will be used to load the contents of the asset when it’s used as an input to other assets inside a job.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_ResourceDefinition_](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) _\]_ _\]_) – beta (Beta) resource definitions that may be required by the [`dagster.IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) provided in the io\_manager\_def argument.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – The description of the asset.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the asset.\
- **observe\_fn** ( _Optional_ _\[_ _SourceAssetObserveFunction_ _\]_)\
- **op\_tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary of tags for the op that computes the asset. Frameworks may expect and require certain metadata to be attached to a op. Values that are not strings will be json encoded and must meet the criteria that json.loads(json.dumps(value)) == value.\
- **auto\_observe\_interval\_minutes** ( _Optional_ _\[_ _float_ _\]_) – While the asset daemon is turned on, a run of the observation function for this asset will be launched at this interval. observe\_fn must be provided.\
- **freshness\_policy** ( _FreshnessPolicy_) – deprecated A constraint telling Dagster how often this asset is intended to be updated with respect to its root data.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
\
`property` is\_observable\
\
Whether the asset is observable.\
\
Type: bool\
\
`property` op\
\
The OpDefinition associated with the observation function of an observable\
source asset.\
\
Throws an error if the asset is not observable.\
\
Type: [OpDefinition](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition)\
\
@dagster.observable\_source\_asset  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/source_asset_decorator.py#L57)\
\
beta\
\
This API is currently in beta, and may have breaking changes in minor version releases, with behavior changes in patch releases.\
\
Create a SourceAsset with an associated observation function.\
\
The observation function of a source asset is wrapped inside of an op and can be executed as\
part of a job. Each execution generates an AssetObservation event associated with the source\
asset. The source asset observation function should return a `DataVersion`,\
a ~dagster.DataVersionsByPartition, or an [`ObserveResult`](https://docs.dagster.io/api/dagster/assets#dagster.ObserveResult).\
\
Parameters:\
\
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the source asset. If not provided, defaults to the name of the decorated function. The asset’s name must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, the source asset’s key is the concatenation of the key\_prefix and the asset’s name, which defaults to the name of the decorated function. Each item in key\_prefix must be a valid name in dagster (ie only contains letters, numbers, and \_) and may not contain python reserved keywords.\
- **metadata** ( _Mapping_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_) – Metadata associated with the asset.\
- **io\_manager\_key** ( _Optional_ _\[_ _str_ _\]_) – The key for the IOManager that will be used to load the contents of the source asset when it’s used as an input to other assets inside a job.\
- **io\_manager\_def** ( _Optional_ _\[_ [_IOManagerDefinition_](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) _\]_) – beta (Beta) The definition of the IOManager that will be used to load the contents of the source asset when it’s used as an input to other assets inside a job.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – The description of the asset.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – A string name used to organize multiple assets into groups. If not provided, the name “default” is used.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Set of resource keys required by the observe op.\
- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_ResourceDefinition_](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) _\]_ _\]_) – beta (Beta) resource definitions that may be required by the [`dagster.IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) provided in the io\_manager\_def argument.\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines the set of partition keys that compose the asset.\
- **op\_tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary of tags for the op that computes the asset. Frameworks may expect and require certain metadata to be attached to a op. Values that are not strings will be json encoded and must meet the criteria that json.loads(json.dumps(value)) == value.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for filtering and organizing. These tags are not attached to runs of the asset.\
- **observe\_fn** ( _Optional_ _\[_ _SourceAssetObserveFunction_ _\]_) – Observation function for the source asset.\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – A condition describing when Dagster should materialize this asset.\
\
`class` dagster.ObserveResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/result.py#L78)\
\
An object representing a successful observation of an asset. These can be returned from an\
@observable\_source\_asset decorated function to pass metadata.\
\
Parameters:\
\
- **asset\_key** ( _Optional_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – The asset key. Optional to include.\
- **metadata** ( _Optional_ _\[_ _RawMetadataMapping_ _\]_) – Metadata to record with the corresponding AssetObservation event.\
- **check\_results** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckResult_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult) _\]_ _\]_) – Check results to record with the corresponding AssetObservation event.\
- **data\_version** ( _Optional_ _\[_ _DataVersion_ _\]_) – The data version of the asset that was observed.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags to record with the corresponding AssetObservation event.\
\
## Dependencies [​](https://docs.dagster.io/api/dagster/assets\#dependencies "Direct link to Dependencies")\
\
`class` dagster.AssetDep  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_dep.py#L26)\
\
Specifies a dependency on an upstream asset.\
\
Parameters:\
\
- **asset** ( _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _str_ _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_) – The upstream asset to depend on.\
- **partition\_mapping** ( _Optional_ _\[_ [_PartitionMapping_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionMapping) _\]_) – Defines what partitions to depend on in the upstream asset. If not provided and the upstream asset is partitioned, defaults to the default partition mapping for the partitions definition, which is typically maps partition keys to the same partition keys in upstream assets.\
\
Examples:\
\
```codeBlockLines_e6Vv\
upstream_asset = AssetSpec("upstream_asset")\
downstream_asset = AssetSpec(\
    "downstream_asset",\
    deps=[\
        AssetDep(\
            upstream_asset,\
            partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)\
        )\
    ]\
)\
\
```\
\
`class` dagster.AssetIn  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_in.py#L17)\
\
Defines an asset dependency.\
\
Parameters:\
\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – If provided, the asset’s key is the concatenation of the key\_prefix and the input name. Only one of the “key\_prefix” and “key” arguments should be provided.\
- **key** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – The asset’s key. Only one of the “key\_prefix” and “key” arguments should be provided.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of the metadata for the input. For example, if you only need a subset of columns from an upstream table, you could include that in metadata and the IO manager that loads the upstream table could use the metadata to determine which columns to load.\
- **partition\_mapping** ( _Optional_ _\[_ [_PartitionMapping_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionMapping) _\]_) – Defines what partitions to depend on in the upstream asset. If not provided, defaults to the default partition mapping for the partitions definition, which is typically maps partition keys to the same partition keys in upstream assets.\
- **dagster\_type** ( [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType)) – Allows specifying type validation functions that will be executed on the input of the decorated function before it runs.\
\
## Asset jobs [​](https://docs.dagster.io/api/dagster/assets\#asset-jobs "Direct link to Asset jobs")\
\
[Asset jobs](https://docs.dagster.io/guides/build/assets/asset-jobs) enable the automation of asset materializations. Dagster’s [asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax) can be used to select assets and assign them to a job.\
\
dagster.define\_asset\_job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/unresolved_asset_job_definition.py#L242)\
\
Creates a definition of a job which will either materialize a selection of assets or observe\
a selection of source assets. This will only be resolved to a JobDefinition once placed in a\
code location.\
\
Parameters:\
\
- **name** ( _str_) – The name for the job.\
\
- **selection** ( _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_) –\
\
The assets that will be materialized or observed when the job is run.\
\
The selected assets must all be included in the assets that are passed to the assets argument of the Definitions object that this job is included on.\
\
The string “my\_asset\*” selects my\_asset and all downstream assets within the code location. A list of strings represents the union of all assets selected by strings within the list.\
\
- **config** –\
\
Describes how the Job is parameterized at runtime.\
\
If no value is provided, then the schema for the job’s run config is a standard format based on its ops and resources.\
\
If a dictionary is provided, then it must conform to the standard config schema, and it will be used as the job’s run config for the job whenever the job is executed. The values provided will be viewable and editable in the Dagster UI, so be careful with secrets.\
\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that annotate the job and can be used for searching and filtering in the UI. Values that are not already strings will be serialized as JSON. If run\_tags is not set, then the content of tags will also be automatically appended to the tags of any runs of this job.\
\
- **run\_tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that will be automatically attached to runs launched by this job. Values that are not already strings will be serialized as JSON. These tag values may be overwritten by tag values provided at invocation time. If run\_tags is set, then tags are not automatically appended to the tags of any runs of this job.\
\
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_ _\]_) – Arbitrary metadata about the job. Keys are displayed string labels, and values are one of the following: string, float, int, JSON-serializable dict, JSON-serializable list, and one of the data classes returned by a MetadataValue static method.\
\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A description for the Job.\
\
- **executor\_def** ( _Optional_ _\[_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_) – How this Job will be executed. Defaults to [`multi_or_in_process_executor`](https://docs.dagster.io/api/dagster/execution#dagster.multi_or_in_process_executor), which can be switched between multi-process and in-process modes of execution. The default mode of execution is multi-process.\
\
- **op\_retry\_policy** ( _Optional_ _\[_ [_RetryPolicy_](https://docs.dagster.io/api/dagster/ops#dagster.RetryPolicy) _\]_) – The default retry policy for all ops that compute assets in this job. Only used if retry policy is not defined on the asset definition.\
\
- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – deprecated (Deprecated) Defines the set of partitions for this job. Deprecated because partitioning is inferred from the selected assets, so setting this is redundant.\
\
\
Returns: The job, which can be placed inside a code location.Return type: UnresolvedAssetJobDefinition\
Examples:\
\
```codeBlockLines_e6Vv\
# A job that targets all assets in the code location:\
@asset\
def asset1():\
    ...\
\
defs = Definitions(\
    assets=[asset1],\
    jobs=[define_asset_job("all_assets")],\
)\
\
# A job that targets a single asset\
@asset\
def asset1():\
    ...\
\
defs = Definitions(\
    assets=[asset1],\
    jobs=[define_asset_job("all_assets", selection=[asset1])],\
)\
\
# A job that targets all the assets in a group:\
defs = Definitions(\
    assets=assets,\
    jobs=[define_asset_job("marketing_job", selection=AssetSelection.groups("marketing"))],\
)\
\
@observable_source_asset\
def source_asset():\
    ...\
\
# A job that observes a source asset:\
defs = Definitions(\
    assets=assets,\
    jobs=[define_asset_job("observation_job", selection=[source_asset])],\
)\
\
# Resources are supplied to the assets, not the job:\
@asset(required_resource_keys={"slack_client"})\
def asset1():\
    ...\
\
defs = Definitions(\
    assets=[asset1],\
    jobs=[define_asset_job("all_assets")],\
    resources={"slack_client": prod_slack_client},\
)\
\
```\
\
`class` dagster.AssetSelection  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L55)\
\
An AssetSelection defines a query over a set of assets and asset checks, normally all that are defined in a code location.\
\
You can use the “\|”, “&”, and “-” operators to create unions, intersections, and differences of selections, respectively.\
\
AssetSelections are typically used with [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job).\
\
By default, selecting assets will also select all of the asset checks that target those assets.\
\
Examples:\
\
```codeBlockLines_e6Vv\
# Select all assets in group "marketing":\
AssetSelection.groups("marketing")\
\
# Select all assets in group "marketing", as well as the asset with key "promotion":\
AssetSelection.groups("marketing") | AssetSelection.assets("promotion")\
\
# Select all assets in group "marketing" that are downstream of asset "leads":\
AssetSelection.groups("marketing") & AssetSelection.assets("leads").downstream()\
\
# Select a list of assets:\
AssetSelection.assets(*my_assets_list)\
\
# Select all assets except for those in group "marketing"\
AssetSelection.all() - AssetSelection.groups("marketing")\
\
# Select all assets which are materialized by the same op as "projections":\
AssetSelection.assets("projections").required_multi_asset_neighbors()\
\
# Select all assets in group "marketing" and exclude their asset checks:\
AssetSelection.groups("marketing") - AssetSelection.all_asset_checks()\
\
# Select all asset checks that target a list of assets:\
AssetSelection.checks_for_assets(*my_assets_list)\
\
# Select a specific asset check:\
AssetSelection.checks(my_asset_check)\
\
```\
\
`static` all  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L96)\
\
Returns a selection that includes all assets and their asset checks.\
\
Parameters: **include\_sources** ( _bool_) – beta If True, then include all external assets.\
\
`static` all\_asset\_checks  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L107)\
\
Returns a selection that includes all asset checks.\
\
`static` assets  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L113)\
\
Returns a selection that includes all of the provided assets and asset checks that target\
them.\
\
Parameters: **\*assets\_defs** ( _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – The assets to\
select.\
Examples:\
\
```codeBlockLines_e6Vv\
AssetSelection.assets(AssetKey(["a"]))\
\
AssetSelection.assets("a")\
\
AssetSelection.assets(AssetKey(["a"]), AssetKey(["b"]))\
\
AssetSelection.assets("a", "b")\
\
@asset\
def asset1():\
    ...\
\
AssetSelection.assets(asset1)\
\
asset_key_list = [AssetKey(["a"]), AssetKey(["b"])]\
AssetSelection.assets(*asset_key_list)\
\
```\
\
`static` checks  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L304)\
\
Returns a selection that includes all of the provided asset checks or check keys.\
\
`static` checks\_for\_assets  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L289)\
\
Returns a selection with the asset checks that target the provided assets.\
\
Parameters: **\*assets\_defs** ( _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – The assets to\
select checks for.\
\
`static` groups  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L231)\
\
Returns a selection that includes materializable assets that belong to any of the\
provided groups and all the asset checks that target them.\
\
Parameters: **include\_sources** ( _bool_) – beta If True, then include external assets matching the group in the\
selection.\
\
`static` key\_prefixes  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L179)\
\
Returns a selection that includes assets that match any of the provided key prefixes and all the asset checks that target them.\
\
Parameters: **include\_sources** ( _bool_) – beta If True, then include external assets matching the key prefix(es)\
in the selection.\
Examples:\
\
```codeBlockLines_e6Vv\
# match any asset key where the first segment is equal to "a" or "b"\
# e.g. AssetKey(["a", "b", "c"]) would match, but AssetKey(["abc"]) would not.\
AssetSelection.key_prefixes("a", "b")\
\
# match any asset key where the first two segments are ["a", "b"] or ["a", "c"]\
AssetSelection.key_prefixes(["a", "b"], ["a", "c"])\
\
```\
\
`static` keys  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L147)\
\
deprecated\
\
This API will be removed in version 2.0.\
Use AssetSelection.assets instead..\
\
Returns a selection that includes assets with any of the provided keys and all asset\
checks that target them.\
\
Deprecated: use AssetSelection.assets instead.\
\
Examples:\
\
```codeBlockLines_e6Vv\
AssetSelection.keys(AssetKey(["a"]))\
\
AssetSelection.keys("a")\
\
AssetSelection.keys(AssetKey(["a"]), AssetKey(["b"]))\
\
AssetSelection.keys("a", "b")\
\
asset_key_list = [AssetKey(["a"]), AssetKey(["b"])]\
AssetSelection.keys(*asset_key_list)\
\
```\
\
`static` tag  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L245)\
\
Returns a selection that includes materializable assets that have the provided tag, and\
all the asset checks that target them.\
\
Parameters: **include\_sources** ( _bool_) – beta If True, then include external assets matching the group in the\
selection.\
\
downstream  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L319)\
\
Returns a selection that includes all assets that are downstream of any of the assets in\
this selection, selecting the assets in this selection by default. Includes the asset checks targeting the returned assets. Iterates through each\
asset in this selection and returns the union of all downstream assets.\
\
depth (Optional\[int\]): If provided, then only include assets to the given depth. A depth\
of 2 means all assets that are children or grandchildren of the assets in this\
selection.\
\
include\_self (bool): If True, then include the assets in this selection in the result.\
If the include\_self flag is False, return each downstream asset that is not part of the\
original selection. By default, set to True.\
\
materializable  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L394)\
\
Given an asset selection, returns a new asset selection that contains all of the assets\
that are materializable. Removes any assets which are not materializable.\
\
required\_multi\_asset\_neighbors  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L372)\
\
Given an asset selection in which some assets are output from a multi-asset compute op\
which cannot be subset, returns a new asset selection that contains all of the assets\
required to execute the original asset selection. Includes the asset checks targeting the returned assets.\
\
roots  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L380)\
\
Given an asset selection, returns a new asset selection that contains all of the root\
assets within the original asset selection. Includes the asset checks targeting the returned assets.\
\
A root asset is an asset that has no upstream dependencies within the asset selection.\
The root asset can have downstream dependencies outside of the asset selection.\
\
Because mixed selections of external and materializable assets are currently not supported,\
keys corresponding to external assets will not be included as roots. To select external assets,\
use the upstream\_source\_assets method.\
\
sinks  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L362)\
\
Given an asset selection, returns a new asset selection that contains all of the sink\
assets within the original asset selection. Includes the asset checks targeting the returned assets.\
\
A sink asset is an asset that has no downstream dependencies within the asset selection.\
The sink asset can have downstream dependencies outside of the asset selection.\
\
sources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L401)\
\
deprecated\
\
This API will be removed in version 2.0.\
Use AssetSelection.roots instead..\
\
Given an asset selection, returns a new asset selection that contains all of the root\
assets within the original asset selection. Includes the asset checks targeting the returned assets.\
\
A root asset is a materializable asset that has no upstream dependencies within the asset\
selection. The root asset can have downstream dependencies outside of the asset selection.\
\
Because mixed selections of external and materializable assets are currently not supported,\
keys corresponding to external assets will not be included as roots. To select external assets,\
use the upstream\_source\_assets method.\
\
upstream  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L338)\
\
Returns a selection that includes all materializable assets that are upstream of any of\
the assets in this selection, selecting the assets in this selection by default. Includes\
the asset checks targeting the returned assets. Iterates through each asset in this\
selection and returns the union of all upstream assets.\
\
Because mixed selections of external and materializable assets are currently not supported,\
keys corresponding to external assets will not be included as upstream of regular assets.\
\
Parameters:\
\
- **depth** ( _Optional_ _\[_ _int_ _\]_) – If provided, then only include assets to the given depth. A depth of 2 means all assets that are parents or grandparents of the assets in this selection.\
- **include\_self** ( _bool_) – If True, then include the assets in this selection in the result. If the include\_self flag is False, return each upstream asset that is not part of the original selection. By default, set to True.\
\
upstream\_source\_assets  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L416)\
\
Given an asset selection, returns a new asset selection that contains all of the external\
assets that are parents of assets in the original selection. Includes the asset checks\
targeting the returned assets.\
\
without\_checks  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_selection.py#L424)\
\
Removes all asset checks in the selection.\
\
## Code locations [​](https://docs.dagster.io/api/dagster/assets\#code-locations "Direct link to Code locations")\
\
Loading assets and asset jobs into a [code location](https://docs.dagster.io/guides/deploy/code-locations/) makes them available to Dagster tools like the UI, CLI, and GraphQL API.\
\
dagster.load\_assets\_from\_modules  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_assets_from_modules.py#L55)\
\
Constructs a list of assets and source assets from the given modules.\
\
Parameters:\
\
- **modules** ( _Iterable_ _\[_ _ModuleType_ _\]_) – The Python modules to look for assets inside.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – Group name to apply to the loaded assets. The returned assets will be copies of the loaded objects, with the group name added.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – Prefix to prepend to the keys of the loaded assets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
- **freshness\_policy** ( _Optional_ _\[_ _FreshnessPolicy_ _\]_) – FreshnessPolicy to apply to all the loaded assets.\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – AutomationCondition to apply to all the loaded assets.\
- **backfill\_policy** ( _Optional_ _\[_ _AutoMaterializePolicy_ _\]_) – BackfillPolicy to apply to all the loaded assets.\
- **source\_key\_prefix** ( _bool_) – Prefix to prepend to the keys of loaded SourceAssets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
\
Returns: A list containing assets and source assets defined in the given modules.Return type: Sequence\[Union\[ [AssetsDefinition](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [SourceAsset](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset)\]\]\
\
dagster.load\_assets\_from\_current\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_assets_from_modules.py#L125)\
\
Constructs a list of assets, source assets, and cacheable assets from the module where\
this function is called.\
\
Parameters:\
\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – Group name to apply to the loaded assets. The returned assets will be copies of the loaded objects, with the group name added.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – Prefix to prepend to the keys of the loaded assets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
- **freshness\_policy** ( _Optional_ _\[_ _FreshnessPolicy_ _\]_) – FreshnessPolicy to apply to all the loaded assets.\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – AutomationCondition to apply to all the loaded assets.\
- **backfill\_policy** ( _Optional_ _\[_ _AutoMaterializePolicy_ _\]_) – BackfillPolicy to apply to all the loaded assets.\
- **source\_key\_prefix** ( _bool_) – Prefix to prepend to the keys of loaded SourceAssets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
\
Returns: A list containing assets, source assets, and cacheable assets defined in the module.Return type: Sequence\[Union\[ [AssetsDefinition](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [SourceAsset](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset), CachableAssetsDefinition\]\]\
\
dagster.load\_assets\_from\_package\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_assets_from_modules.py#L177)\
\
Constructs a list of assets and source assets that includes all asset\
definitions, source assets, and cacheable assets in all sub-modules of the given package module.\
\
A package module is the result of importing a package.\
\
Parameters:\
\
- **package\_module** ( _ModuleType_) – The package module to looks for assets inside.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – Group name to apply to the loaded assets. The returned assets will be copies of the loaded objects, with the group name added.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – Prefix to prepend to the keys of the loaded assets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
- **freshness\_policy** ( _Optional_ _\[_ _FreshnessPolicy_ _\]_) – FreshnessPolicy to apply to all the loaded assets.\
- **automation\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – AutomationCondition to apply to all the loaded assets.\
- **backfill\_policy** ( _Optional_ _\[_ _AutoMaterializePolicy_ _\]_) – BackfillPolicy to apply to all the loaded assets.\
- **source\_key\_prefix** ( _bool_) – Prefix to prepend to the keys of loaded SourceAssets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
\
Returns: A list containing assets, source assets, and cacheable assets defined in the module.Return type: Sequence\[Union\[ [AssetsDefinition](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [SourceAsset](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset), CacheableAssetsDefinition\]\]\
\
dagster.load\_assets\_from\_package\_name  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_assets_from_modules.py#L227)\
\
Constructs a list of assets, source assets, and cacheable assets that includes all asset\
definitions and source assets in all sub-modules of the given package.\
\
Parameters:\
\
- **package\_name** ( _str_) – The name of a Python package to look for assets inside.\
- **group\_name** ( _Optional_ _\[_ _str_ _\]_) – Group name to apply to the loaded assets. The returned assets will be copies of the loaded objects, with the group name added.\
- **key\_prefix** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – Prefix to prepend to the keys of the loaded assets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
- **freshness\_policy** ( _Optional_ _\[_ _FreshnessPolicy_ _\]_) – FreshnessPolicy to apply to all the loaded assets.\
- **auto\_materialize\_policy** ( _Optional_ _\[_ _AutoMaterializePolicy_ _\]_) – AutoMaterializePolicy to apply to all the loaded assets.\
- **backfill\_policy** ( _Optional_ _\[_ _AutoMaterializePolicy_ _\]_) – BackfillPolicy to apply to all the loaded assets.\
- **source\_key\_prefix** ( _bool_) – Prefix to prepend to the keys of loaded SourceAssets. The returned assets will be copies of the loaded objects, with the prefix prepended.\
\
Returns: A list containing assets, source assets, and cacheable assets defined in the module.Return type: Sequence\[Union\[ [AssetsDefinition](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [SourceAsset](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset), CacheableAssetsDefinition\]\]\
\
## Observations [​](https://docs.dagster.io/api/dagster/assets\#observations "Direct link to Observations")\
\
Refer to the [Asset observation](https://docs.dagster.io/guides/build/assets/metadata-and-tags/asset-observations) documentation for more information.\
\
`class` dagster.AssetObservation  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/events.py#L362)\
\
Event that captures metadata about an asset at a point in time.\
\
Parameters:\
\
- **asset\_key** ( _Union_ _\[_ _str_ _,_ _List_ _\[_ _str_ _\]_ _,_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – A key to identify the asset.\
- **partition** ( _Optional_ _\[_ _str_ _\]_) – The name of a partition of the asset that the metadata corresponds to.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A mapping containing tags for the observation.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Union_ _\[_ _str_ _,_ _float_ _,_ _int_ _,_ [_MetadataValue_](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) _\]_ _\]_ _\]_) – Arbitrary metadata about the asset. Keys are displayed string labels, and values are one of the following: string, float, int, JSON-serializable dict, JSON-serializable list, and one of the data classes returned by a MetadataValue static method.\
\
## Declarative Automation [​](https://docs.dagster.io/api/dagster/assets\#declarative-automation "Direct link to Declarative Automation")\
\
Refer to the [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation/) documentation for more information.\
\
`class` dagster.AutomationCondition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L52)\
\
An AutomationCondition represents a condition of an asset that impacts whether it should be\
automatically executed. For example, you can have a condition which becomes true whenever the\
code version of the asset is changed, or whenever an upstream dependency is updated.\
\
```codeBlockLines_e6Vv\
from dagster import AutomationCondition, asset\
\
@asset(automation_condition=AutomationCondition.on_cron("0 0 * * *"))\
def my_asset(): ...\
\
```\
\
AutomationConditions may be combined together into expressions using a variety of operators.\
\
```codeBlockLines_e6Vv\
from dagster import AssetSelection, AutomationCondition, asset\
\
# any dependencies from the "important" group are missing\
any_important_deps_missing = AutomationCondition.any_deps_match(\
    AutomationCondition.missing(),\
).allow(AssetSelection.groups("important"))\
\
# there is a new code version for this asset since the last time it was requested\
new_code_version = AutomationCondition.code_version_changed().since(\
    AutomationCondition.newly_requested()\
)\
\
# there is a new code version and no important dependencies are missing\
my_condition = new_code_version & ~any_important_deps_missing\
\
@asset(automation_condition=my_condition)\
def my_asset(): ...\
\
```\
\
`static` all\_checks\_match  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L367)\
\
Returns an AutomationCondition that is true for an asset partition if all of its checks\
evaluate to True for the given condition.\
\
Parameters:\
\
- **condition** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – The AutomationCondition that will be evaluated against this asset’s checks.\
- **blocking\_only** ( _bool_) – Determines if this condition will only be evaluated against blocking checks. Defaults to False.\
\
`static` all\_deps\_blocking\_checks\_passed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L633)\
\
Returns an AutomationCondition that is true for any partition where all upstream\
blocking checks have passed, or will be requested on this tick.\
\
In-tick requests are allowed to enable creating runs that target both a parent with\
blocking checks and a child. Even though the checks have not currently passed, if\
they fail within the run, the run machinery will prevent the child from being\
materialized.\
\
`static` all\_deps\_match  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L335)\
\
Returns an AutomationCondition that is true for a if at least one partition\
of the all of the target’s dependencies evaluate to True for the given condition.\
\
Parameters: **condition** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – The AutomationCondition that will be evaluated against\
this target’s dependencies.\
\
`static` all\_deps\_updated\_since\_cron  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L651)\
\
Returns an AutomatonCondition that is true if all of the target’s dependencies have\
updated since the latest tick of the provided cron schedule.\
\
`static` any\_checks\_match  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L349)\
\
Returns an AutomationCondition that is true for if at least one of the target’s\
checks evaluate to True for the given condition.\
\
Parameters:\
\
- **condition** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – The AutomationCondition that will be evaluated against this asset’s checks.\
- **blocking\_only** ( _bool_) – Determines if this condition will only be evaluated against blocking checks. Defaults to False.\
\
`static` any\_deps\_in\_progress  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L623)\
\
Returns an AutomationCondition that is true if the target has at least one dependency\
that is in progress.\
\
`static` any\_deps\_match  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L321)\
\
Returns an AutomationCondition that is true for a if at least one partition\
of the any of the target’s dependencies evaluate to True for the given condition.\
\
Parameters: **condition** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – The AutomationCondition that will be evaluated against\
this target’s dependencies.\
\
`static` any\_deps\_missing  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L613)\
\
Returns an AutomationCondition that is true if the target has at least one dependency\
that is missing, and will not be requested on this tick.\
\
`static` any\_deps\_updated  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L593)\
\
Returns an AutomationCondition that is true if the target has at least one dependency\
that has updated since the previous tick, or will be requested on this tick.\
\
Will ignore parent updates if the run that updated the parent also plans to update\
the asset or check that this condition is applied to.\
\
`static` any\_downstream\_conditions  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L726)\
\
beta\
\
This API is currently in beta, and may have breaking changes in minor version releases, with behavior changes in patch releases.\
\
Returns an AutomationCondition which represents the union of all distinct downstream conditions.\
\
`static` asset\_matches  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L308)\
\
Returns an AutomationCondition that is true if this condition is true for the given key.\
\
`static` backfill\_in\_progress  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L405)\
\
Returns an AutomationCondition that is true if the target is part of an in-progress backfill.\
\
`static` check\_failed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L445)\
\
Returns an AutomationCondition that is true for an asset check if it has evaluated against\
the latest materialization of an asset and failed.\
\
`static` check\_passed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L435)\
\
Returns an AutomationCondition that is true for an asset check if it has evaluated against\
the latest materialization of an asset and passed.\
\
`static` code\_version\_changed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L546)\
\
Returns an AutomationCondition that is true if the target’s code version has been changed\
since the previous tick.\
\
`static` cron\_tick\_passed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L570)\
\
Returns an AutomationCondition that is whenever a cron tick of the provided schedule is passed.\
\
`static` data\_version\_changed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L558)\
\
Returns an AutomationCondition that is true if the target’s data version has been changed\
since the previous tick.\
\
`static` eager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L666)\
\
Returns an AutomationCondition which will cause a target to be executed if any of\
its dependencies update, and will execute missing partitions if they become missing\
after this condition is applied to the target.\
\
This will not execute targets that have any missing or in progress dependencies, or\
are currently in progress.\
\
For time partitioned assets, only the latest time partition will be considered.\
\
`static` execution\_failed  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L415)\
\
Returns an AutomationCondition that is true if the latest execution of the target failed.\
\
`static` in\_latest\_time\_window  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L465)\
\
Returns an AutomationCondition that is true when the target it is within the latest\
time window.\
\
Parameters: **lookback\_delta** ( _Optional_ _,_ _datetime.timedelta_) – If provided, the condition will\
return all partitions within the provided delta of the end of the latest time window.\
For example, if this is used on a daily-partitioned asset with a lookback\_delta of\
48 hours, this will return the latest two partitions.\
\
`static` in\_progress  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L425)\
\
Returns an AutomationCondition that is true for an asset partition if it is part of an\
in-progress run or backfill.\
\
`static` initial\_evaluation  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L455)\
\
Returns an AutomationCondition that is true on the first evaluation of the expression.\
\
`static` missing  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L385)\
\
Returns an AutomationCondition that is true if the target has not been executed.\
\
`static` newly\_missing  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L587)\
\
Returns an AutomationCondition that is true on the tick that the target becomes missing.\
\
`static` newly\_requested  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L536)\
\
Returns an AutomationCondition that is true if the target was requested on the previous tick.\
\
`static` newly\_updated  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L495)\
\
Returns an AutomationCondition that is true if the target has been updated since the previous tick.\
\
`static` on\_cron  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L688)\
\
Returns an AutomationCondition which will cause a target to be executed on a given\
cron schedule, after all of its dependencies have been updated since the latest\
tick of that cron schedule.\
\
For time partitioned assets, only the latest time partition will be considered.\
\
`static` on\_missing  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L705)\
\
Returns an AutomationCondition which will execute partitions of the target that\
are added after this condition is applied to the asset.\
\
This will not execute targets that have any missing dependencies.\
\
For time partitioned assets, only the latest time partition will be considered.\
\
`static` run\_in\_progress  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L395)\
\
Returns an AutomationCondition that is true if the target is part of an in-progress run.\
\
`static` will\_be\_requested  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L485)\
\
Returns an AutomationCondition that is true if the target will be requested this tick.\
\
replace  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L293)\
\
Replaces all instances of `old` across any sub-conditions with `new`.\
\
If `old` is a string, then conditions with a label matching\
that string will be replaced.\
\
Parameters:\
\
- **old** ( _Union_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _,_ _str_ _\]_) – The condition to replace.\
- **new** ( [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition)) – The condition to replace with.\
\
`class` dagster.AutomationResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/declarative_automation/automation_condition.py#L753)\
\
The result of evaluating an AutomationCondition.\
\
`class` dagster.AutomationConditionSensorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/automation_condition_sensor_definition.py#L78)\
\
Targets a set of assets and repeatedly evaluates all the AutomationConditions on all of\
those assets to determine which to request runs for.\
\
Parameters:\
\
- **name** – The name of the sensor.\
- **target** ( _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_) – A selection of assets to evaluate AutomationConditions of and request runs for.\
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.\
- **run\_tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Tags that will be automatically attached to runs launched by this sensor.\
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.\
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.\
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The frequency at which to try to evaluate the sensor. The actual interval will be longer if the sensor evaluation takes longer than the provided interval.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.\
- **emit\_backfills** ( _bool_) – If set to True, will emit a backfill on any tick where more than one partition of any single asset is requested, rather than individual runs. Defaults to True.\
- **use\_user\_code\_server** ( _bool_) – beta (Beta) If set to True, this sensor will be evaluated in the user code server, rather than the AssetDaemon. This enables evaluating custom AutomationCondition subclasses, and ensures that the condition definitions will remain in sync with your user code version, eliminating version skew. Note: currently a maximum of 500 assets or checks may be targeted at a time by a sensor that has this value set.\
- **default\_condition** ( _Optional_ _\[_ [_AutomationCondition_](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) _\]_) – beta (Beta) If provided, this condition will be used for any selected assets or asset checks which do not have an automation condition defined. Requires use\_user\_code\_server to be set to True.\
\
Examples:\
\
```codeBlockLines_e6Vv\
import dagster as dg\
\
# automation condition sensor that defaults to running\
defs1 = dg.Definitions(\
    assets=...,\
    sensors=[\
        dg.AutomationConditionSensorDefinition(\
            name="automation_condition_sensor",\
            target=dg.AssetSelection.all(),\
            default_status=dg.DefaultSensorStatus.RUNNING,\
        ),\
    ]\
)\
\
# one automation condition sensor per group\
defs2 = dg.Definitions(\
    assets=...,\
    sensors=[\
        dg.AutomationConditionSensorDefinition(\
            name="raw_data_automation_condition_sensor",\
            target=dg.AssetSelection.groups("raw_data"),\
        ),\
        dg.AutomationConditionSensorDefinition(\
            name="ml_automation_condition_sensor",\
            target=dg.AssetSelection.groups("machine_learning"),\
        ),\
    ]\
)\
\
```\
\
## Asset values [​](https://docs.dagster.io/api/dagster/assets\#asset-values "Direct link to Asset values")\
\
`class` dagster.AssetValueLoader  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/asset_value_loader.py#L29)\
\
Caches resource definitions that are used to load asset values across multiple load\
invocations.\
\
Should not be instantiated directly. Instead, use\
[`get_asset_value_loader()`](https://docs.dagster.io/api/dagster/repositories#dagster.RepositoryDefinition.get_asset_value_loader).\
\
load\_asset\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/asset_value_loader.py#L71)\
\
Loads the contents of an asset as a Python object.\
\
Invokes load\_input on the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) associated with the asset.\
\
Parameters:\
\
- **asset\_key** ( _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _str_ _\]_) – The key of the asset to load.\
- **python\_type** ( _Optional_ _\[_ _Type_ _\]_) – The python type to load the asset as. This is what will be returned inside load\_input by context.dagster\_type.typing\_type.\
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The partition of the asset to load.\
- **input\_definition\_metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Input metadata to pass to the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) (is equivalent to setting the metadata argument in In or AssetIn).\
- **resource\_config** ( _Optional_ _\[_ _Any_ _\]_) – A dictionary of resource configurations to be passed to the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).\
\
Returns: The contents of an asset as a Python object.\
\
- [Asset definitions](https://docs.dagster.io/api/dagster/assets#asset-definitions)\
- [Graph-backed asset definitions](https://docs.dagster.io/api/dagster/assets#graph-backed-asset-definitions)\
- [Multi-asset definitions](https://docs.dagster.io/api/dagster/assets#multi-asset-definitions)\
- [Source assets](https://docs.dagster.io/api/dagster/assets#source-assets)\
- [Dependencies](https://docs.dagster.io/api/dagster/assets#dependencies)\
- [Asset jobs](https://docs.dagster.io/api/dagster/assets#asset-jobs)\
- [Code locations](https://docs.dagster.io/api/dagster/assets#code-locations)\
- [Observations](https://docs.dagster.io/api/dagster/assets#observations)\
- [Declarative Automation](https://docs.dagster.io/api/dagster/assets#declarative-automation)\
- [Asset values](https://docs.dagster.io/api/dagster/assets#asset-values)\
\

```

### File: curated_dagster_docs/foundation/api_quick_index/definitions.md
```markdown

`class` dagster.Definitions  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L313)

A set of definitions explicitly available and loadable by Dagster tools.

Parameters:

- **assets** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _,_ _CacheableAssetsDefinition_ _\]_ _\]_ _\]_) – A list of assets. Assets can be created by annotating a function with [`@asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) or [`@observable_source_asset`](https://docs.dagster.io/api/dagster/assets#dagster.observable_source_asset). Or they can by directly instantiating [`AssetsDefinition`](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [`SourceAsset`](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset), or `CacheableAssetsDefinition`.
- **asset\_checks** ( _Optional_ _\[_ _Iterable_ _\[_ [_AssetChecksDefinition_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetChecksDefinition) _\]_ _\]_) – A list of asset checks.
- **schedules** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_ScheduleDefinition_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) _,_ _UnresolvedPartitionedAssetScheduleDefinition_ _\]_ _\]_ _\]_) – List of schedules.
- **sensors** ( _Optional_ _\[_ _Iterable_ _\[_ [_SensorDefinition_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SensorDefinition) _\]_ _\]_) – List of sensors, typically created with [`@sensor`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.sensor).
- **jobs** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – List of jobs. Typically created with [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) or with [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) for jobs defined in terms of ops directly. Jobs created with [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) must already have resources bound at job creation time. They do not respect the resources argument here.
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets. The resources dictionary takes raw Python objects, not just instances of [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). If that raw object inherits from [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager), it gets coerced to an [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition). Any other object is coerced to a [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). These resources will be automatically bound to any assets passed to this Definitions instance using [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources). Assets passed to Definitions with resources already bound using [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources) will override this dictionary.
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _,_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _\]_ _\]_) – Default executor for jobs. Individual jobs can override this and define their own executors by setting the executor on [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) explicitly. This executor will also be used for materializing assets directly outside of the context of jobs. If an [`Executor`](https://docs.dagster.io/api/dagster/internals#dagster.Executor) is passed, it is coerced into an [`ExecutorDefinition`](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition).
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_) – Default loggers for jobs. Individual jobs can define their own loggers by setting them explictly.\
- **metadata** ( _Optional_ _\[_ _MetadataMapping_ _\]_) – Arbitrary metadata for the Definitions. Not displayed in the UI but accessible on the Definitions instance at runtime.\
\
Example usage:\
\
```codeBlockLines_e6Vv\
defs = Definitions(\
    assets=[asset_one, asset_two],\
    schedules=[a_schedule],\
    sensors=[a_sensor],\
    jobs=[a_job],\
    resources={\
        "a_resource": some_resource,\
    },\
    asset_checks=[asset_one_check_one]\
)\
\
```\
\
Dagster separates user-defined code from system tools such the web server and\
the daemon. Rather than loading code directly into process, a tool such as the\
webserver interacts with user-defined code over a serialization boundary.\
\
These tools must be able to locate and load this code when they start. Via CLI\
arguments or config, they specify a Python module to inspect.\
\
A Python module is loadable by Dagster tools if there is a top-level variable\
that is an instance of [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
\
`static` merge  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L591)\
\
Merges multiple Definitions objects into a single Definitions object.\
\
The returned Definitions object has the union of all the definitions in the input\
Definitions objects.\
\
Raises an error if the Definitions objects to be merged contain conflicting values for the\
same resource key or logger key, or if they have different executors defined.\
\
Examples:\
\
```codeBlockLines_e6Vv\
import submodule1\
import submodule2\
\
defs = Definitions.merge(submodule1.defs, submodule2.defs)\
\
```\
\
Returns: The merged definitions.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
`static` validate\_loadable  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L576)\
\
Validates that the enclosed definitions will be loadable by Dagster:\
\
- No assets have conflicting keys.\
- No jobs, sensors, or schedules have conflicting names.\
- All asset jobs can be resolved.\
- All resource requirements are satisfied.\
\
Meant to be used in unit tests.\
\
Raises an error if any of the above are not true.\
\
get\_all\_asset\_specs  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L675)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Returns an AssetSpec object for every asset contained inside the Definitions object.\
\
get\_asset\_value\_loader  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L502)\
\
Returns an object that can load the contents of assets as Python objects.\
\
Invokes load\_input on the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) associated with the assets. Avoids\
spinning up resources separately for each asset.\
\
Usage:\
\
```codeBlockLines_e6Vv\
with defs.get_asset_value_loader() as loader:\
    asset1 = loader.load_asset_value("asset1")\
    asset2 = loader.load_asset_value("asset2")\
\
```\
\
get\_job\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L438)\
\
Get a job definition by name. If you passed in a an `UnresolvedAssetJobDefinition`\
(return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) it will be resolved to a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) when returned\
from this function, with all resource dependencies fully resolved.\
\
get\_schedule\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L456)\
\
Get a [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) by name.\
If your passed-in schedule had resource dependencies, or the job targeted by the schedule had\
resource dependencies, those resource dependencies will be fully resolved on the returned object.\
\
get\_sensor\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L447)\
\
Get a [`SensorDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SensorDefinition) by name.\
If your passed-in sensor had resource dependencies, or the job targeted by the sensor had\
resource dependencies, those resource dependencies will be fully resolved on the returned object.\
\
load\_asset\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L465)\
\
Load the contents of an asset as a Python object.\
\
Invokes load\_input on the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) associated with the asset.\
\
If you want to load the values of multiple assets, it’s more efficient to use\
[`get_asset_value_loader()`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions.get_asset_value_loader), which avoids spinning up\
resources separately for each asset.\
\
Parameters:\
\
- **asset\_key** ( _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _str_ _\]_) – The key of the asset to load.\
- **python\_type** ( _Optional_ _\[_ _Type_ _\]_) – The python type to load the asset as. This is what will be returned inside load\_input by context.dagster\_type.typing\_type.\
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The partition of the asset to load.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Input metadata to pass to the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) (is equivalent to setting the metadata argument in In or AssetIn).\
\
Returns: The contents of an asset as a Python object.\
\
map\_asset\_specs  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L711)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Map a function over the included AssetSpecs or AssetsDefinitions in this Definitions object, replacing specs in the sequence\
or specs in an AssetsDefinitions with the result of the function.\
\
Parameters:\
\
- **func** ( _Callable_ _\[_ _\[_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_) – The function to apply to each AssetSpec.\
- **selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) – An asset selection to narrow down the set of assets to apply the function to. If not provided, applies to all assets.\
\
Returns: A Definitions object where the AssetSpecs have been replaced with the result of the function where the selection applies.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
Examples:\
\
```codeBlockLines_e6Vv\
import dagster as dg\
\
my_spec = dg.AssetSpec("asset1")\
\
@dg.asset\
def asset2(_): ...\
\
defs = Definitions(\
    assets=[asset1, asset2]\
)\
\
# Applies to asset1 and asset2\
mapped_defs = defs.map_asset_specs(\
    func=lambda s: s.merge_attributes(metadata={"new_key": "new_value"}),\
)\
\
# Applies only to asset1\
mapped_defs = defs.map_asset_specs(\
    func=lambda s: s.replace_attributes(metadata={"new_key": "new_value"}),\
    selection="asset1",\
)\
\
```\
\
dagster.create\_repository\_using\_definitions\_args  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L52)\
\
Create a named repository using the same arguments as [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). In older\
versions of Dagster, repositories were the mechanism for organizing assets, schedules, sensors,\
and jobs. There could be many repositories per code location. This was a complicated ontology but\
gave users a way to organize code locations that contained large numbers of heterogenous definitions.\
\
As a stopgap for those who both want to 1) use the new [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) API and 2) but still\
want multiple logical groups of assets in the same code location, we have introduced this function.\
\
Example usage:\
\
```codeBlockLines_e6Vv\
named_repo = create_repository_using_definitions_args(\
    name="a_repo",\
    assets=[asset_one, asset_two],\
    schedules=[a_schedule],\
    sensors=[a_sensor],\
    jobs=[a_job],\
    resources={\
        "a_resource": some_resource,\
    }\
)\
\
```\
\
dagster.load\_definitions\_from\_current\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L78)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the module where this function is called.\
\
Parameters:\
\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the current module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L49)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given module.\
\
Parameters:\
\
- **module** ( _ModuleType_) – The Python module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_modules  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L17)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given modules.\
\
Parameters:\
\
- **modules** ( _Iterable_ _\[_ _ModuleType_ _\]_) – The Python modules to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given modules.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_package\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L110)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given package module.\
\
Parameters:\
\
- **package\_module** ( _ModuleType_) – The package module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given package module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_package\_name  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L143)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the package module for the given package name.\
\
Parameters:\
\
- **package\_name** ( _str_) – The name of the package module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the package module for the given package name.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\

```

### File: curated_dagster_docs/foundation/api_quick_index/resources.md
```markdown

On this page

## Pythonic resource system [​](https://docs.dagster.io/api/dagster/resources\#pythonic-resource-system "Direct link to Pythonic resource system")

The following classes are used as part of the new [Pythonic resources system](https://docs.dagster.io/guides/build/external-resources/).

`class` dagster.ConfigurableResource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/resource.py#L595)

Base class for Dagster resources that utilize structured config.

This class is a subclass of both [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) and [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config).

Example definition:

```codeBlockLines_e6Vv
class WriterResource(ConfigurableResource):
    prefix: str

    def output(self, text: str) -> None:
        print(f"{self.prefix}{text}")

```

Example usage:

```codeBlockLines_e6Vv
@asset
def asset_that_uses_writer(writer: WriterResource):
    writer.output("text")

defs = Definitions(
    assets=[asset_that_uses_writer],
    resources={"writer": WriterResource(prefix="a_prefix")},
)

```

You can optionally use this class to model configuration only and vend an object
of a different type for use at runtime. This is useful for those who wish to
have a separate object that manages configuration and a separate object at runtime. Or
where you want to directly use a third-party class that you do not control.

To do this you override the create\_resource methods to return a different object.

```codeBlockLines_e6Vv
class WriterResource(ConfigurableResource):
    prefix: str

    def create_resource(self, context: InitResourceContext) -> Writer:
        # Writer is pre-existing class defined else
        return Writer(self.prefix)

```

Example usage:

```codeBlockLines_e6Vv
@asset
def use_preexisting_writer_as_resource(writer: ResourceParam[Writer]):
    writer.output("text")

defs = Definitions(
    assets=[use_preexisting_writer_as_resource],
    resources={"writer": WriterResource(prefix="a_prefix")},
)

```

`class` dagster.ResourceDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L57)

Core class for defining resources.

Resources are scoped ways to make external resources (like database connections) available to
ops and assets during job execution and to clean up after execution resolves.

If resource\_fn yields once rather than returning (in the manner of functions decorable with
`@contextlib.contextmanager`) then the body of the
function after the yield will be run after execution resolves, allowing users to write their
own teardown/cleanup logic.

Depending on your executor, resources may be instantiated and cleaned up more than once in a
job execution.

Parameters:

- **resource\_fn** ( _Callable_ _\[_ _\[_ [_InitResourceContext_](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) _\]_ _,_ _Any_ _\]_) – User-provided function to instantiate the resource, which will be made available to executions keyed on the `context.resources` object.
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema)) – The schema for the config. If set, Dagster will check that config provided for the resource matches this schema and fail if it does not. If not set, Dagster will accept any config provided for the resource.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.\
- **required\_resource\_keys** – (Optional\[Set\[str\]\]) Keys for the resources required by this resource. A DagsterInvariantViolationError will be raised during initialization if dependencies are cyclic.\
- **version** ( _Optional_ _\[_ _str_ _\]_) – beta (Beta) The version of the resource’s definition fn. Two wrapped resource functions should only have the same version if they produce the same resource definition when provided with the same inputs.\
\
`static` hardcoded\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L175)\
\
A helper function that creates a `ResourceDefinition` with a hardcoded object.\
\
Parameters:\
\
- **value** ( _Any_) – The value that will be accessible via context.resources.resource\_name.\
- **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.\
\
Returns: A hardcoded resource.Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` mock\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L197)\
\
A helper function that creates a `ResourceDefinition` which wraps a `mock.MagicMock`.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.Returns:\
A resource that creates the magic methods automatically and helps\
you mock existing resources.\
\
Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` none\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L162)\
\
A helper function that returns a none resource.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.Returns: A resource that does nothing.Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` string\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L215)\
\
Creates a `ResourceDefinition` which takes in a single string as configuration\
and returns this configured string to any ops or assets which depend on it.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the string resource. Defaults to None.Returns:\
A resource that takes in a single string as configuration and\
returns that string.\
\
Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`property` description\
\
A human-readable description of the resource.\
\
`property` required\_resource\_keys\
\
A set of the resource keys that this resource depends on. These keys will be made available\
to the resource’s init context during execution, and the resource will not be instantiated\
until all required resources are available.\
\
`property` version\
\
A string which can be used to identify a particular code version of a resource definition.\
\
`class` dagster.InitResourceContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/init.py#L18)\
\
The context object available as the argument to the initialization function of a [`dagster.ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
Users should not instantiate this object directly. To construct an InitResourceContext for testing purposes, use [`dagster.build_init_resource_context()`](https://docs.dagster.io/api/dagster/resources#dagster.build_init_resource_context).\
\
Example:\
\
```codeBlockLines_e6Vv\
from dagster import resource, InitResourceContext\
\
@resource\
def the_resource(init_context: InitResourceContext):\
    init_context.log.info("Hello, world!")\
\
```\
\
`property` instance\
\
The Dagster instance configured for the current execution context.\
\
`property` log\
\
The Dagster log manager configured for the current execution context.\
\
`property` log\_manager\
\
The log manager for this run of the job.\
\
`property` resource\_config\
\
The configuration data provided by the run config. The schema\
for this data is defined by the `config_field` argument to\
[`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
`property` resource\_def\
\
The definition of the resource currently being constructed.\
\
`property` resources\
\
The resources that are available to the resource that we are initializing.\
\
`property` run\
\
The dagster run to use. When initializing resources outside of execution context, this will be None.\
\
dagster.make\_values\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L418)\
\
A helper function that creates a `ResourceDefinition` to take in user-defined values.\
\
This is useful for sharing values between ops.\
\
Parameters: **\*\*kwargs** – Arbitrary keyword arguments that will be passed to the config schema of the\
returned resource definition. If not set, Dagster will accept any config provided for\
the resource.\
For example:\
\
```codeBlockLines_e6Vv\
@op(required_resource_keys={"globals"})\
def my_op(context):\
    print(context.resources.globals["my_str_var"])\
\
@job(resource_defs={"globals": make_values_resource(my_str_var=str, my_int_var=int)})\
def my_job():\
    my_op()\
\
```\
\
Returns: A resource that passes in user-defined values.Return type: [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\
\
dagster.build\_init\_resource\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/init.py#L257)\
\
Builds resource initialization context from provided parameters.\
\
`build_init_resource_context` can be used as either a function or context manager. If there is a\
provided resource to `build_init_resource_context` that is a context manager, then it must be\
used as a context manager. This function can be used to provide the context argument to the\
invocation of a resource.\
\
Parameters:\
\
- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources to provide to the context. These can be either values or resource definitions.\
- **config** ( _Optional_ _\[_ _Any_ _\]_) – The resource config to provide to the context.\
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured for the context. Defaults to DagsterInstance.ephemeral().\
\
Examples:\
\
```codeBlockLines_e6Vv\
context = build_init_resource_context()\
resource_to_init(context)\
\
with build_init_resource_context(\
    resources={"foo": context_manager_resource}\
) as context:\
    resource_to_init(context)\
\
```\
\
dagster.build\_resources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/build_resources.py#L42)\
\
Context manager that yields resources using provided resource definitions and run config.\
\
This API allows for using resources in an independent context. Resources will be initialized\
with the provided run config, and optionally, dagster\_run. The resulting resources will be\
yielded on a dictionary keyed identically to that provided for resource\_defs. Upon exiting the\
context, resources will also be torn down safely.\
\
Parameters:\
\
- **resources** ( _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) – Resource instances or definitions to build. All required resource dependencies to a given resource must be contained within this dictionary, or the resource build will fail.\
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured to instantiate resources on.\
- **resource\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict representing the config to be provided to each resource during initialization and teardown.\
- **dagster\_run** ( _Optional_ _\[_ _PipelineRun_ _\]_) – The pipeline run to provide during resource initialization and teardown. If the provided resources require either the dagster\_run or run\_id attributes of the provided context during resource initialization and/or teardown, this must be provided, or initialization will fail.\
- **log\_manager** ( _Optional_ _\[_ [_DagsterLogManager_](https://docs.dagster.io/api/dagster/loggers#dagster.DagsterLogManager) _\]_) – Log Manager to use during resource initialization. Defaults to system log manager.\
- **event\_loop** ( _Optional_ _\[_ _AbstractEventLoop_ _\]_) – An event loop for handling resources with async context managers.\
\
Examples:\
\
```codeBlockLines_e6Vv\
from dagster import resource, build_resources\
\
@resource\
def the_resource():\
    return "foo"\
\
with build_resources(resources={"from_def": the_resource, "from_val": "bar"}) as resources:\
    assert resources.from_def == "foo"\
    assert resources.from_val == "bar"\
\
```\
\
dagster.with\_resources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/with_resources.py#L15)\
\
Adds dagster resources to copies of resource-requiring dagster definitions.\
\
An error will be thrown if any provided definitions have a conflicting\
resource definition provided for a key provided to resource\_defs. Resource\
config can be provided, with keys in the config dictionary corresponding to\
the keys for each resource definition. If any definition has unsatisfied\
resource keys after applying with\_resources, an error will be thrown.\
\
Parameters:\
\
- **definitions** ( _Iterable_ _\[_ _ResourceAddable_ _\]_) – Dagster definitions to provide resources to.\
- **resource\_defs** ( _Mapping_ _\[_ _str_ _,_ _object_ _\]_) – Mapping of resource keys to objects to satisfy resource requirements of provided dagster definitions.\
- **resource\_config\_by\_key** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Specifies config for provided resources. The key in this dictionary corresponds to configuring the same key in the resource\_defs dictionary.\
\
Examples:\
\
```codeBlockLines_e6Vv\
from dagster import asset, resource, with_resources\
\
@resource(config_schema={"bar": str})\
def foo_resource():\
    ...\
\
@asset(required_resource_keys={"foo"})\
def asset1(context):\
    foo = context.resources.foo\
    ...\
\
@asset(required_resource_keys={"foo"})\
def asset2(context):\
    foo = context.resources.foo\
    ...\
\
asset1_with_foo, asset2_with_foo = with_resources(\
    [asset1, asset2],\
    resource_defs={\
        "foo": foo_resource\
    },\
    resource_config_by_key={\
        "foo": {\
            "config": {"bar": ...}\
        }\
    }\
)\
\
```\
\
## Utilities [​](https://docs.dagster.io/api/dagster/resources\#utilities "Direct link to Utilities")\
\
`class` dagster.EnvVar  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/field_utils.py#L523)\
\
Class used to represent an environment variable in the Dagster config system.\
\
This class is intended to be used to populate config fields or resources.\
The environment variable will be resolved to a string value when the config is\
loaded.\
\
To access the value of the environment variable, use the get\_value method.\
\
## Legacy resource system [​](https://docs.dagster.io/api/dagster/resources\#legacy-resource-system "Direct link to Legacy resource system")\
\
The following classes are used as part of the [legacy resource system](https://legacy-docs.dagster.io/concepts/resources-legacy).\
\
@dagster.resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L375)\
\
Define a resource.\
\
The decorated function should accept an [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) and return an instance of\
the resource. This function will become the `resource_fn` of an underlying\
[`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
If the decorated function yields once rather than returning (in the manner of functions\
decorable with `@contextlib.contextmanager`) then\
the body of the function after the yield will be run after execution resolves, allowing users\
to write their own teardown/cleanup logic.\
\
Parameters:\
\
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for the config. Configuration data available in init\_context.resource\_config. If not set, Dagster will accept any config provided.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.\
- **version** ( _Optional_ _\[_ _str_ _\]_) – beta (Beta) The version of a resource function. Two wrapped resource functions should only have the same version if they produce the same resource definition when provided with the same inputs.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Keys for the resources required by this resource.\
\
- [Pythonic resource system](https://docs.dagster.io/api/dagster/resources#pythonic-resource-system)\
- [Utilities](https://docs.dagster.io/api/dagster/resources#utilities)\
- [Legacy resource system](https://docs.dagster.io/api/dagster/resources#legacy-resource-system)\
\

```

### File: curated_dagster_docs/foundation/concepts_overview/concepts.md
```markdown

On this page

Dagster provides a variety of abstractions for building and orchestrating data pipelines. These concepts enable a modular, declarative approach to data engineering, making it easier to manage dependencies, monitor execution, and ensure data quality.

Component

Definitions

AssetCheck

Asset

Config

Code Location

Graph

IO Manager

Job

Op

Partition

Resource

Schedule

Sensor

Type

### Asset [​](https://docs.dagster.io/getting-started/concepts\#asset "Direct link to Asset")

Config

Partition

Job

Schedule

Sensor

IOManager

Resource

AssetCheck

Definitions

Asset

An [`asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) represents a logical unit of data such as a table, dataset, or machine learning model. Assets can have dependencies on other assets, forming the data lineage for your pipelines. As the core abstraction in Dagster, assets can interact with many other Dagster concepts to facilitate certain tasks.

| Concept | Relationship |
| --- | --- |
| [asset check](https://docs.dagster.io/getting-started/concepts#asset-check) | `asset` may use an `asset check` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `asset` may use a `config` |
| [io manager](https://docs.dagster.io/getting-started/concepts#io-manager) | `asset` may use a `io manager` |
| [partition](https://docs.dagster.io/getting-started/concepts#partition) | `asset` may use a `partition` |
| [resource](https://docs.dagster.io/getting-started/concepts#resource) | `asset` may use a `resource` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `asset` may be used in a `job` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `asset` may be used in a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `asset` may be used in a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `asset` must be set in a `definitions` to be deployed |

### Asset Check [​](https://docs.dagster.io/getting-started/concepts\#asset-check "Direct link to Asset Check")

Asset

Asset Check

Definitions

An [`asset_check`](https://docs.dagster.io/api/dagster/asset-checks#dagster.asset_check) is associated with an [`asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) to ensure it meets certain expectations around data quality, freshness or completeness. Asset checks run when the asset is executed and store metadata about the related run and if all the conditions of the check were met.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `asset check` may be used by an `asset` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `asset check` must be set in a `definitions` to be deployed |

### Code Location [​](https://docs.dagster.io/getting-started/concepts\#code-location "Direct link to Code Location")

Definitions

Code Location

A `code location` is a collection of [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) deployed in a specific environment. A code location determines the Python environment (including the version of Dagster being used as well as any other Python dependencies). A Dagster project can have multiple code locations, helping isolate dependencies.

| Concept | Relationship |
| --- | --- |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `code location` must contain at least one `definitions` |

### Component [​](https://docs.dagster.io/getting-started/concepts\#component "Direct link to Component")

Component

Definitions

A `Component` is an opinionated project layout built around a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). The [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) contains Dagster objects used to accomplish a specific task—such as a standard workflow or an integration. Components are designed to help you quickly bootstrap parts of your Dagster project and serve as templates for repeatable patterns.

| Concept | Relationship |
| --- | --- |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `component` produces a `definitions` |

### Config [​](https://docs.dagster.io/getting-started/concepts\#config "Direct link to Config")

Config

Job

Asset

Schedule

Sensor

A [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig) is a set schema applied to a Dagster object that is input at the time of execution. This allows for parameterization and the reuse of pipelines to serve multiple purposes.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `config` may be used by an `asset` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `config` may be used by a `job` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `config` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `config` may be used by a `sensor` |

### Definitions [​](https://docs.dagster.io/getting-started/concepts\#definitions "Direct link to Definitions")

Job

Schedule

Asset

Sensor

IOManager

Resource

AssetCheck

CodeLocation

Definitions

[`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) is a top-level construct that contains
references to all the objects of a Dagster project, such as
[`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset),
[`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) and
[`ScheduleDefinitions`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition). Only objects included
in the definitions will be deployed and visible within the Dagster UI.

A [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) can also be constructed from a `Component` which is a predefined set of Dagster objects.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `definitions` may contain one or more `assets` |
| [asset check](https://docs.dagster.io/getting-started/concepts#asset-check) | `definitions` may contain one or more `asset checks` |
| [io manager](https://docs.dagster.io/getting-started/concepts#io-manager) | `definitions` may contain one or more `io managers` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `definitions` may contain one or more `jobs` |
| [resource](https://docs.dagster.io/getting-started/concepts#resource) | `definitions` may contain one or more `resources` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `definitions` may contain one or more `schedules` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `definitions` may contain one or more `sensors` |
| [component](https://docs.dagster.io/getting-started/concepts#component) | `definitions` may exist as the output of a `component` |
| [code location](https://docs.dagster.io/getting-started/concepts#code-location) | `definitions` must be deployed in a `code location` |

### Graph [​](https://docs.dagster.io/getting-started/concepts\#graph "Direct link to Graph")

Config

Op

Graph

Job

A [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) connects multiple [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op) together to form a DAG. If you are using [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset), you will not need to use graphs directly.

| Concept | Relationship |
| --- | --- |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `graph` may use a `config` |
| [op](https://docs.dagster.io/getting-started/concepts#op) | `graph` must include one or more `ops` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `graph` must be part of `job` to execute |

### IO Manager [​](https://docs.dagster.io/getting-started/concepts\#io-manager "Direct link to IO Manager")

Asset

IO Manager

Definitions

An [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) defines how data is stored and retrieved between the execution of [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) and [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op). This allows for a customizable storage and format at any interaction in a pipeline.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `io manager` may be used by an `asset` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `io manager` must be set in a `definitions` to be deployed |

### Job [​](https://docs.dagster.io/getting-started/concepts\#job "Direct link to Job")

Asset

Graph

Config

Schedule

Sensor

Definitions

Job

A [`job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) is a subset of [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) or the [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) of [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op). Jobs are the main form of execution in Dagster.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `job` may contain a selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `job` may use a `config` |
| [graph](https://docs.dagster.io/getting-started/concepts#graph) | `job` may contain a `graph` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `job` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `job` may be used by a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `job` must be set in a `definitions` to be deployed |

### Op [​](https://docs.dagster.io/getting-started/concepts\#op "Direct link to Op")

Type

Op

Graph

An [`op`](https://docs.dagster.io/api/dagster/ops#dagster.op) is a computational unit of work. Ops are arranged into a [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) to dictate their order. Ops have largely been replaced by [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset).

| Concept | Relationship |
| --- | --- |
| [type](https://docs.dagster.io/getting-started/concepts#type) | `op` may use a `type` |
| [graph](https://docs.dagster.io/getting-started/concepts#graph) | `op` must be contained in `graph` to execute |

### Partition [​](https://docs.dagster.io/getting-started/concepts\#partition "Direct link to Partition")

Partition

Asset

A [`PartitionsDefinition`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) represents a logical slice of a dataset or computation mapped to a certain segments (such as increments of time). Partitions enable incremental processing, making workflows more efficient by only running on relevant subsets of data.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `partition` may be used by an `asset` |

### Resource [​](https://docs.dagster.io/getting-started/concepts\#resource "Direct link to Resource")

Resource

Asset

Schedule

Sensor

Definitions

A [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) is a configurable external dependency. These can be databases, APIs, or anything outside of Dagster.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `resource` may be used by an `asset` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `resource` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `resource` may be used by a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `resource` must be set in a `definitions` to be deployed |

### Type [​](https://docs.dagster.io/getting-started/concepts\#type "Direct link to Type")

Type

Op

A `type` is a way to define and validate the data passed between [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op).

| Concept | Relationship |
| --- | --- |
| [op](https://docs.dagster.io/getting-started/concepts#op) | `type` may be used by an `op` |

### Schedule [​](https://docs.dagster.io/getting-started/concepts\#schedule "Direct link to Schedule")

Asset

Config

Definitions

Job

Schedule

A [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) is a way to automate [`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) to occur on a specified interval. In the cases that a job or asset is parameterized, the schedule can also be set with a run configuration ( [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig)) to match.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `schedule` may include a `job` or selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `schedule` may include a `config` if the `job` or `assets` include a `config` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `schedule` may include a `job` or selection of `assets` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `schedule` must be set in a `definitions` to be deployed |

### Sensor [​](https://docs.dagster.io/getting-started/concepts\#sensor "Direct link to Sensor")

Asset

Config

Definitions

Job

Sensor

A `sensor` is a way to trigger [`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) when an event occurs, such as a file being uploaded or a push notification. In the cases that a job or asset is parameterized, the sensor can also be set with a run configuration ( [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig)) to match.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `sensor` may include a `job` or selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `sensor` may include a `config` if the `job` or `assets` include a `config` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `sensor` may include a `job` or selection of `assets` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `sensor` must be set in a `definitions` to be deployed |

- [Asset](https://docs.dagster.io/getting-started/concepts#asset)
- [Asset Check](https://docs.dagster.io/getting-started/concepts#asset-check)
- [Code Location](https://docs.dagster.io/getting-started/concepts#code-location)
- [Component](https://docs.dagster.io/getting-started/concepts#component)
- [Config](https://docs.dagster.io/getting-started/concepts#config)
- [Definitions](https://docs.dagster.io/getting-started/concepts#definitions)
- [Graph](https://docs.dagster.io/getting-started/concepts#graph)
- [IO Manager](https://docs.dagster.io/getting-started/concepts#io-manager)
- [Job](https://docs.dagster.io/getting-started/concepts#job)
- [Op](https://docs.dagster.io/getting-started/concepts#op)
- [Partition](https://docs.dagster.io/getting-started/concepts#partition)
- [Resource](https://docs.dagster.io/getting-started/concepts#resource)
- [Type](https://docs.dagster.io/getting-started/concepts#type)
- [Schedule](https://docs.dagster.io/getting-started/concepts#schedule)
- [Sensor](https://docs.dagster.io/getting-started/concepts#sensor)

```

### File: curated_dagster_docs/foundation/core/defining_assets/defining-assets-with-asset-dependencies.md
```markdown

On this page

Asset definitions can depend on other asset definitions. The dependent asset is called the **downstream asset**, and the asset it depends on is the **upstream asset**.

## Defining basic dependencies [​](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies\#defining-basic-dependencies "Direct link to Defining basic dependencies")

You can define a dependency between two assets by passing the upstream asset to the `deps` parameter in the downstream asset's `@asset` decorator.

In this example, the asset `sugary_cereals` creates a new table ( `sugary_cereals`) by selecting records from the `cereals` table. Then the asset `shopping_list` creates a new table ( `shopping_list`) by selecting records from `sugary_cereals`:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def sugary_cereals() -> None:
    execute_query(
        "CREATE TABLE sugary_cereals AS SELECT * FROM cereals WHERE sugar_grams > 10"
    )

@dg.asset(deps=[sugary_cereals])
def shopping_list() -> None:
    execute_query("CREATE TABLE shopping_list AS SELECT * FROM sugary_cereals")

```

## Defining asset dependencies across code locations [​](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies\#defining-asset-dependencies-across-code-locations "Direct link to Defining asset dependencies across code locations")

Assets can depend on assets in different [code locations](https://docs.dagster.io/guides/deploy/code-locations/). In the following example, the `code_location_1_asset` asset produces a JSON string from a file in `code_location_1`:

```codeBlockLines_e6Vv
import json

import dagster as dg

@dg.asset
def code_location_1_asset():
    with open("/tmp/data/code_location_1_asset.json", "w+") as f:
        json.dump(5, f)

defs = dg.Definitions(assets=[code_location_1_asset])

```

In `code_location_2`, we can reference `code_location_1_asset` it via its asset key:

```codeBlockLines_e6Vv
import json

import dagster as dg

@dg.asset(deps=["code_location_1_asset"])
def code_location_2_asset():
    with open("/tmp/data/code_location_1_asset.json") as f:
        x = json.load(f)

    with open("/tmp/data/code_location_2_asset.json", "w+") as f:
        json.dump(x + 6, f)

```

- [Defining basic dependencies](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies#defining-basic-dependencies)
- [Defining asset dependencies across code locations](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies#defining-asset-dependencies-across-code-locations)

```

### File: curated_dagster_docs/foundation/core/defining_assets/defining-assets.md
```markdown

On this page

The most common way to create a data asset in Dagster is by annotating a Python function with an [`@dg.asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) decorator. The function computes the contents of the asset, such as a database table or file.

An asset definition includes the following:

- An `AssetKey`, which is a handle for referring to the asset.
- A set of upstream asset keys, which refer to assets that the contents of the asset definition are derived from.
- A Python function, which is responsible for computing the contents of the asset from its upstream dependencies and storing the results.

Prerequisites

To run the code in this article, you'll need to install Dagster. For more information, see the [Installation guide](https://docs.dagster.io/getting-started/installation).

## Asset decorators [​](https://docs.dagster.io/guides/build/assets/defining-assets\#asset-decorators "Direct link to Asset decorators")

Dagster has four types of asset decorators:

| Decorator | Description |
| --- | --- |
| `@asset` | Defines a single asset. [See an example](https://docs.dagster.io/guides/build/assets/defining-assets#single-asset). |
| `@multi_asset` | Outputs multiple assets from a single operation. [See an example](https://docs.dagster.io/guides/build/assets/defining-assets#multi-asset). |
| `@graph_asset` | Outputs a single asset from multiple operations without making each operation itself an asset. [See an example](https://docs.dagster.io/guides/build/assets/defining-assets#graph-asset). |
| `@graph_multi_asset` | Outputs multiple assets from multiple operations |

## Defining operations that create a single asset [​](https://docs.dagster.io/guides/build/assets/defining-assets\#single-asset "Direct link to Defining operations that create a single asset")

The simplest way to define a data asset in Dagster is by using the [`@dg.asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) decorator. This decorator marks a Python function as an asset.

Using @dg.asset decorator

```codeBlockLines_e6Vv
from typing import List

import dagster as dg

@dg.asset
def daily_sales() -> None: ...

@dg.asset(deps=[daily_sales], group_name="sales")
def weekly_sales() -> None: ...

@dg.asset(
    deps=[weekly_sales],
    owners=["bighead@hooli.com", "team:roof", "team:corpdev"],
)
def weekly_sales_report(context: dg.AssetExecutionContext):
    context.log.info("Loading data for my_dataset")

defs = dg.Definitions(assets=[daily_sales, weekly_sales, weekly_sales_report])

```

In this example, `weekly_sales_report` is an asset that logs its output. Dagster automatically tracks its dependencies and handles its execution within the pipeline.

## Defining operations that create multiple assets [​](https://docs.dagster.io/guides/build/assets/defining-assets\#multi-asset "Direct link to Defining operations that create multiple assets")

When you need to generate multiple assets from a single operation, you can use the [`@dg.multi_asset`](https://docs.dagster.io/api/dagster/assets#dagster.multi_asset) decorator. This allows you to output multiple assets while maintaining a single processing function, which could be useful for:

- Making a single call to an API that updates multiple tables
- Using the same in-memory object to compute multiple assets

In this example, `my_multi_asset` produces two assets: `asset_one` and `asset_two`. Each is derived from the same function, which makes it easier to handle related data transformations together:

Using @dg.multi\_asset decorator

```codeBlockLines_e6Vv
import dagster as dg

@dg.multi_asset(specs=[dg.AssetSpec("asset_one"), dg.AssetSpec("asset_two")])
def my_multi_asset():
    yield dg.MaterializeResult(asset_key="asset_one", metadata={"num_rows": 10})
    yield dg.MaterializeResult(asset_key="asset_two", metadata={"num_rows": 24})

defs = dg.Definitions(assets=[my_multi_asset])

```

This example could be expressed as:

my\_multi\_asset

asset\_one

asset\_two

## Defining multiple operations that create a single asset [​](https://docs.dagster.io/guides/build/assets/defining-assets\#graph-asset "Direct link to Defining multiple operations that create a single asset")

For cases where you need to perform multiple operations to produce a single asset, you can use the [`@dg.graph_asset`](https://docs.dagster.io/api/dagster/assets#dagster.graph_asset) decorator. This approach encapsulates a series of operations and exposes them as a single asset, allowing you to model complex pipelines while only exposing the final output.

Using @dg.graph\_asset decorator

```codeBlockLines_e6Vv
from random import randint

import dagster as dg

@dg.op(
    retry_policy=dg.RetryPolicy(
        max_retries=5,
        delay=0.2,  # 200ms
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    )
)
def step_one() -> int:
    if randint(0, 2) >= 1:
        raise Exception("Flaky step that may fail randomly")
    return 42

@dg.op
def step_two(num: int):
    return num**2

@dg.graph_asset
def complex_asset():
    return step_two(step_one())

defs = dg.Definitions(assets=[complex_asset])

```

In this example, `complex_asset` is an asset that's the result of two operations: `step_one` and `step_two`. These steps are combined into a single asset, abstracting away the intermediate representations.

This example could be expressed as:

step\_one

complex\_asset

step\_two

## Asset context [​](https://docs.dagster.io/guides/build/assets/defining-assets\#asset-context "Direct link to Asset context")

When defining an asset, you can optionally provide a first parameter, `context`. When this parameter is supplied, Dagster will supply an [`AssetExecutionContext`](https://docs.dagster.io/api/dagster/execution#dagster.AssetExecutionContext) object to the body of the asset which provides access to system information like loggers and the current run ID.

For example, to access the logger and log an info message:

```codeBlockLines_e6Vv
from dagster import AssetExecutionContext, asset

@asset
def context_asset(context: AssetExecutionContext):
    context.log.info(f"My run ID is {context.run.run_id}")
    ...

```

## Asset code versions [​](https://docs.dagster.io/guides/build/assets/defining-assets\#asset-code-versions "Direct link to Asset code versions")

Assets may be assigned a `code_version`. Versions let you help Dagster track what assets haven't been re-materialized since their code has changed, and avoid performing redundant computation.

```codeBlockLines_e6Vv

@asset(code_version="1")
def asset_with_version():
    with open("data/asset_with_version.json", "w") as f:
        json.dump(100, f)

```

When an asset with a code version is materialized, the generated `AssetMaterialization` is tagged with the version. The UI will indicate when an asset has a different code version than the code version used for its most recent materialization.

## Assets with multi-part keys [​](https://docs.dagster.io/guides/build/assets/defining-assets\#assets-with-multi-part-keys "Direct link to Assets with multi-part keys")

Assets are often objects in systems with hierarchical namespaces, like filesystems. Because of this, it often makes sense for an asset key to be a list of strings, instead of just a single string. To define an asset with a multi-part asset key, use the `key_prefix` argument with a list of strings. The full asset key is formed by prepending the `key_prefix` to the asset name (which defaults to the name of the decorated function).

```codeBlockLines_e6Vv
from dagster import AssetIn, asset

@asset(key_prefix=["one", "two", "three"])
def upstream_asset():
    return [1, 2, 3]

@asset(ins={"upstream_asset": AssetIn(key_prefix=["one", "two", "three"])})
def downstream_asset(upstream_asset):
    return upstream_asset + [4]

```

## Next steps [​](https://docs.dagster.io/guides/build/assets/defining-assets\#next-steps "Direct link to Next steps")

- Enrich Dagster's built-in data catalog with [asset metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/)
- Learn to [pass data between assets](https://docs.dagster.io/guides/build/assets/passing-data-between-assets)
- Learn to use a [factory pattern](https://docs.dagster.io/guides/build/assets/creating-asset-factories) to create multiple, similar assets

- [Asset decorators](https://docs.dagster.io/guides/build/assets/defining-assets#asset-decorators)
- [Defining operations that create a single asset](https://docs.dagster.io/guides/build/assets/defining-assets#single-asset)
- [Defining operations that create multiple assets](https://docs.dagster.io/guides/build/assets/defining-assets#multi-asset)
- [Defining multiple operations that create a single asset](https://docs.dagster.io/guides/build/assets/defining-assets#graph-asset)
- [Asset context](https://docs.dagster.io/guides/build/assets/defining-assets#asset-context)
- [Asset code versions](https://docs.dagster.io/guides/build/assets/defining-assets#asset-code-versions)
- [Assets with multi-part keys](https://docs.dagster.io/guides/build/assets/defining-assets#assets-with-multi-part-keys)
- [Next steps](https://docs.dagster.io/guides/build/assets/defining-assets#next-steps)

```

### File: curated_dagster_docs/foundation/core/defining_assets/metadata-and-tags.md
```markdown

On this page

[Assets](https://docs.dagster.io/guides/build/assets/) feature prominently in the Dagster UI. Attaching information to assets allows you to understand where they're stored, what they contain, and how they should be organized.

Using metadata in Dagster, you can:

- Attach ownership information
- Organize assets with tags
- Attach rich, complex information such as a Markdown description, a table schema, or a time series
- Link assets with their source code

## Adding owners to assets [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#owners "Direct link to Adding owners to assets")

In a large organization, it's important to know which individuals and teams are responsible for a given data asset.

```codeBlockLines_e6Vv
import dagster as dg

# You can attach owners via the `owners` argument on the `@asset` decorator.
@dg.asset(owners=["richard.hendricks@hooli.com", "team:data-eng"])
def my_asset(): ...

# You can also use `owners` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset", owners=["bighead@hooli.com", "team:roof", "team:corpdev"]
)

```

note

`owners` must either be an email address or a team name prefixed by `team:` (e.g. `team:data-eng`).

tip

With Dagster+ Pro, you can create asset-based alerts that automatically notify an asset's owners when triggered. Refer to the [Dagster+ alert documentation](https://docs.dagster.io/dagster-plus/features/alerts) for more information.

## Organizing assets with tags [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#tags "Direct link to Organizing assets with tags")

[**Tags**](https://docs.dagster.io/guides/build/assets/metadata-and-tags/tags) are the primary way to organize assets in Dagster. You can attach several tags to an asset when it's defined, and they will appear in the UI. You can also use tags to search and filter for assets in the Asset catalog. They're structured as key-value pairs of strings.

Here's an example of some tags you might apply to an asset:

```codeBlockLines_e6Vv
{"domain": "marketing", "pii": "true"}

```

As with `owners`, you can pass a dictionary of tags to the `tags` argument when defining an asset:

```codeBlockLines_e6Vv
import dagster as dg

# You can attach tags via the `tags` argument on the `@asset` decorator.
@dg.asset(tags={"domain": "marketing", "pii": "true"})
def my_asset(): ...

# You can also use `tags` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset", tags={"domain": "legal", "sensitive": ""}
)

```

Keep in mind that tags must contain only strings as keys and values. Additionally, the Dagster UI will render tags with the empty string as a "label" rather than a key-value pair.

## Attaching metadata to assets [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#attaching-metadata "Direct link to Attaching metadata to assets")

**Metadata** allows you to attach rich information to the asset, like a Markdown description, a table schema, or a time series. Metadata is more flexible than tags, as it can store more complex information.

Metadata can be attached to an asset at definition time, when the code is first imported, or at runtime when an asset is materialized.

### At definition time [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#definition-time-metadata "Direct link to At definition time")

Using definition metadata to describe assets can make it easy to provide context for you and your team. This metadata could be descriptions of the assets, the types of assets, or links to relevant documentation.

```codeBlockLines_e6Vv
import dagster as dg

# You can attach metadata via the `metadata` argument on the `@asset` decorator.
@dg.asset(
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://..."),
        "snippet": dg.MetadataValue.md("# Embedded markdown\n..."),
    }
)
def my_asset(): ...

# You can also use `metadata` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset",
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://..."),
        "snippet": dg.MetadataValue.md("# Embedded markdown\n..."),
    },
)

```

To learn more about the different types of metadata you can attach, see the [`MetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) API docs.

Some metadata keys will be given special treatment in the Dagster UI. See the [Standard metadata types](https://docs.dagster.io/guides/build/assets/metadata-and-tags#standard-metadata-types) section for more information.

### At runtime [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#runtime-metadata "Direct link to At runtime")

With runtime metadata, you can surface information about an asset's materialization, such as how many records were processed or when the materialization occurred. This allows you to update an asset's information when it changes and track historical metadata as a time series.

To attach materialization metadata to an asset, returning a [`MaterializeResult`](https://docs.dagster.io/api/dagster/assets#dagster.MaterializeResult) object containing a `metadata` parameter. This parameter accepts a dictionary of key/value pairs, where keys must be a string.

When specifying values, use the [`MetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) utility class to wrap the data to ensure it displays correctly in the UI. Values can also be primitive Python types, which Dagster will convert to the appropriate `MetadataValue`.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def my_asset():
    # Asset logic goes here
    return dg.MaterializeResult(
        metadata={
            # file_size_kb will be rendered as a time series
            "file_size_kb": dg.MetadataValue.int(...)
        }
    )

```

note

Numerical metadata is treated as a time series in the Dagster UI.

## Standard metadata types [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#standard-metadata-types "Direct link to Standard metadata types")

The following metadata keys are given special treatment in the Dagster UI.

| Key | Description |
| --- | --- |
| `dagster/uri` | **Type:** `str`<br> The URI for the asset, for example: "s3://my\_bucket/my\_object" |
| `dagster/column_schema` | **Type:** [`TableSchema`](https://docs.dagster.io/api/dagster/metadata#dagster.TableSchema)<br> For an asset that's a table, the schema of the columns in the table. Refer to the [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column) section for details. |
| `dagster/column_lineage` | **Type:** [`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage)<br> For an asset that's a table, the lineage of column inputs to column outputs for the table. Refer to the [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column) section for details. |
| `dagster/row_count` | **Type:** `int`<br> For an asset that's a table, the number of rows in the table. Refer to the Table metadata documentation for details. |
| `dagster/partition_row_count` | **Type:** `int`<br> For a partition of an asset that's a table, the number of rows in the partition. |
| `dagster/table_name` | **Type:** `str`<br> A unique identifier for the table/view, typically fully qualified. For example, my\_database.my\_schema.my\_table |
| `dagster/code_references` | **Type:** [`CodeReferencesMetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.CodeReferencesMetadataValue)<br> A list of [code references](https://docs.dagster.io/guides/build/assets/metadata-and-tags#source-code) for the asset, such as file locations or references to GitHub URLs. Should only be provided in definition-level metadata, not materialization metadata. |

## Table and column metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#table-column "Direct link to Table and column metadata")

Two of the most powerful metadata types are [`TableSchema`](https://docs.dagster.io/api/dagster/metadata#dagster.TableSchema) and [`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage). These metadata types allow stakeholders to view the schema of a table right within Dagster, and, in Dagster+, navigate to the [Asset catalog](https://docs.dagster.io/dagster-plus/features/asset-catalog/) with the column lineage.

### Table schema metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#table-schema "Direct link to Table schema metadata")

The following example attaches [table and column schema metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/table-metadata) at both definition time and runtime:

```codeBlockLines_e6Vv
import dagster as dg

# Definition-time metadata
# Here, we know the schema of the asset, so we can attach it to the asset decorator
@dg.asset(
    deps=["source_bar", "source_baz"],
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[\
                dg.TableColumn(\
                    "name",\
                    "string",\
                    description="The name of the person",\
                ),\
                dg.TableColumn(\
                    "age",\
                    "int",\
                    description="The age of the person",\
                ),\
            ]
        )
    },
)
def my_asset(): ...

# Runtime metadata
# Here, the schema isn't known until the asset is materialized
@dg.asset(deps=["source_bar", "source_baz"])
def my_other_asset():
    column_names = ...
    column_types = ...

    columns = [\
        dg.TableColumn(name, column_type)\
        for name, column_type in zip(column_names, column_types)\
    ]

    return dg.MaterializeResult(
        metadata={"dagster/column_schema": dg.TableSchema(columns=columns)}
    )

```

There are several data types and constraints available on [`TableColumn`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumn) objects. For more information, see the API documentation.

### Column lineage metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#column-lineage "Direct link to Column lineage metadata")

tip

Many integrations such as [dbt](https://docs.dagster.io/integrations/libraries/dbt) automatically attach column lineage metadata out-of-the-box.

[Column lineage metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/column-level-lineage) is a powerful way to track how columns in a table are derived from other columns:

Table column lineage metadata

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(deps=["source_bar", "source_baz"])
def my_asset():
    # The following TableColumnLineage models 3 tables:
    # source_bar, source_baz, and my_asset
    # With the following column dependencies:
    # - my_asset.new_column_foo depends on:
    #   - source_bar.column_bar
    #   - source_baz.column_baz
    # - my_asset.new_column_qux depends on:
    #   - source_bar.column_quuz
    return dg.MaterializeResult(
        metadata={
            "dagster/column_lineage": dg.TableColumnLineage(
                deps_by_column={
                    "new_column_foo": [\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_bar"),\
                            column_name="column_bar",\
                        ),\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_baz"),\
                            column_name="column_baz",\
                        ),\
                    ],
                    "new_column_qux": [\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_bar"),\
                            column_name="column_quuz",\
                        ),\
                    ],
                }
            )
        }
    )

```

tip

Dagster+ provides rich visualization and navigation of column lineage in the Asset catalog. Refer to the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/features/asset-catalog/) for more information.

## Linking assets with source code [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#source-code "Direct link to Linking assets with source code")

info

This feature is considered in a beta stage. It is still being tested and may change. For more information, see the [API lifecycle stages documentation](https://docs.dagster.io/api/api-lifecycle/api-lifecycle-stages).

To link assets with their source code, you can attach a code reference. Code references are a type of metadata that allow you to easily view those assets' source code from the Dagster UI, both in local development and in production.

tip

Many integrations, such as [dbt](https://docs.dagster.io/integrations/libraries/dbt/reference#attaching-code-reference-metadata), support this capability.

### Attaching Python code references for local development [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#python-references "Direct link to Attaching Python code references for local development")

Dagster can automatically attach code references to assets during local development:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

defs = dg.Definitions(
    # with_source_code_references() automatically attaches the proper metadata
    assets=dg.with_source_code_references([my_asset, another_asset])
)

```

### Customizing code references [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#custom-references "Direct link to Customizing code references")

If you want to customize how code references are attached - such as when you are building [domain-specific languages with asset factories](https://docs.dagster.io/guides/build/assets/creating-asset-factories) \- you can manually add the `dagster/code_references` metadata to asset definitions:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    metadata={
        "dagster/code_references": dg.CodeReferencesMetadataValue(
            code_references=[\
                dg.LocalFileCodeReference(\
                    file_path="/path/to/source.yaml",\
                    # Label and line number are optional\
                    line_number=1,\
                    label="Model YAML",\
                )\
            ]
        )
    }
)
def my_asset_modeled_in_yaml(): ...

defs = dg.Definitions(assets=dg.with_source_code_references([my_asset_modeled_in_yaml]))

```

### Attaching code references in production [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#production-references "Direct link to Attaching code references in production")

- Dagster+
- OSS

Dagster+ can automatically annotate assets with code references to source control, such as GitHub or GitLab.

```codeBlockLines_e6Vv
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud

import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

defs = dg.Definitions(
    assets=link_code_references_to_git_if_cloud(
        assets_defs=dg.with_source_code_references([my_asset, another_asset]),
        # This function also supports customizable path mapping, but usually
        # the defaults are fine.
    )
)

```

If you aren't using Dagster+, you can annotate your assets with code references to source control, but it requires manual mapping:

```codeBlockLines_e6Vv
import os
from pathlib import Path

import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

assets = dg.with_source_code_references([my_asset, another_asset])

defs = dg.Definitions(
    assets=dg.link_code_references_to_git(
        assets_defs=assets,
        git_url="https://github.com/dagster-io/dagster",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__),
            file_anchor_path_in_repository="src/repo.py",
        ),
    )
    # Only link to GitHub in production. link locally otherwise
    if bool(os.getenv("IS_PRODUCTION"))
    else assets
)

```

`link_code_references_to_git` currently supports GitHub and GitLab repositories. It also supports customization of how file paths are mapped; see the `AnchorBasedFilePathMapping` API docs for more information.

- [Adding owners to assets](https://docs.dagster.io/guides/build/assets/metadata-and-tags#owners)
- [Organizing assets with tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags#tags)
- [Attaching metadata to assets](https://docs.dagster.io/guides/build/assets/metadata-and-tags#attaching-metadata)
  - [At definition time](https://docs.dagster.io/guides/build/assets/metadata-and-tags#definition-time-metadata)
  - [At runtime](https://docs.dagster.io/guides/build/assets/metadata-and-tags#runtime-metadata)
- [Standard metadata types](https://docs.dagster.io/guides/build/assets/metadata-and-tags#standard-metadata-types)
- [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column)
  - [Table schema metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-schema)
  - [Column lineage metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#column-lineage)
- [Linking assets with source code](https://docs.dagster.io/guides/build/assets/metadata-and-tags#source-code)
  - [Attaching Python code references for local development](https://docs.dagster.io/guides/build/assets/metadata-and-tags#python-references)
  - [Customizing code references](https://docs.dagster.io/guides/build/assets/metadata-and-tags#custom-references)
  - [Attaching code references in production](https://docs.dagster.io/guides/build/assets/metadata-and-tags#production-references)

```

### File: curated_dagster_docs/foundation/core/installation_quickstart/installation.md
```markdown

On this page

To follow the steps in this guide, you'll need:

- To install Python 3.9 or higher. **Python 3.12 is recommended**.
- To install pip, a Python package installer

## Setting up a virtual environment [​](https://docs.dagster.io/getting-started/installation\#setting-up-a-virtual-environment "Direct link to Setting up a virtual environment")

After installing Python, it's recommended that you set up a virtual environment. This will isolate your Dagster project from the rest of your system and make it easier to manage dependencies.

There are many ways to do this, but this guide will use `venv` as it doesn't require additional dependencies.

- MacOS
- Windows

`bash python -m venv venv source venv/bin/activate `

`bash python -m venv venv source venv\Scripts\activate `

tip

**Looking for something more powerful than `venv`?** Try `pyenv` or `pyenv-virtualenv`, which can help you manage multiple versions of Python on a single machine. Learn more in the [pyenv GitHub repository](https://github.com/pyenv/pyenv).

## Installing Dagster [​](https://docs.dagster.io/getting-started/installation\#installing-dagster "Direct link to Installing Dagster")

To install Dagster in your virtual environment, open your terminal and run the following command:

```codeBlockLines_e6Vv
pip install dagster dagster-webserver

```

This command will install the core Dagster library and the webserver, which is used to serve the Dagster UI.

## Verifying installation [​](https://docs.dagster.io/getting-started/installation\#verifying-installation "Direct link to Verifying installation")

To verify that Dagster is installed correctly, run the following command:

```codeBlockLines_e6Vv
dagster --version

```

The version numbers of Dagster should be printed in the terminal:

```codeBlockLines_e6Vv
> dagster --version
dagster, version 1.8.4

```

## Troubleshooting [​](https://docs.dagster.io/getting-started/installation\#troubleshooting "Direct link to Troubleshooting")

If you encounter any issues during the installation process:

- Refer to the [Dagster GitHub repository](https://github.com/dagster-io/dagster) for troubleshooting, or
- Reach out to the [Dagster community](https://docs.dagster.io/about/community)

## Next steps [​](https://docs.dagster.io/getting-started/installation\#next-steps "Direct link to Next steps")

- Get up and running with your first Dagster project in the [Quickstart](https://docs.dagster.io/getting-started/quickstart)
- Learn to [create data assets in Dagster](https://docs.dagster.io/guides/build/assets/defining-assets)

- [Setting up a virtual environment](https://docs.dagster.io/getting-started/installation#setting-up-a-virtual-environment)
- [Installing Dagster](https://docs.dagster.io/getting-started/installation#installing-dagster)
- [Verifying installation](https://docs.dagster.io/getting-started/installation#verifying-installation)
- [Troubleshooting](https://docs.dagster.io/getting-started/installation#troubleshooting)
- [Next steps](https://docs.dagster.io/getting-started/installation#next-steps)

```

### File: curated_dagster_docs/foundation/core/installation_quickstart/quickstart.md
```markdown

On this page

Welcome to Dagster! In this guide, you'll use Dagster to create a basic pipeline that:

- Extracts data from a CSV file
- Transforms the data
- Loads the transformed data to a new CSV file

## What you'll learn [​](https://docs.dagster.io/getting-started/quickstart\#what-youll-learn "Direct link to What you'll learn")

- How to set up a basic Dagster project
- How to create a single Dagster asset that encapsulates the entire Extract, Transform, and Load (ETL) process
- How to use Dagster's UI to monitor and execute your pipeline

## Prerequisites [​](https://docs.dagster.io/getting-started/quickstart\#prerequisites "Direct link to Prerequisites")

Prerequisites

To follow the steps in this guide, you'll need:

- Basic Python knowledge
- Python 3.9+ installed on your system. Refer to the [Installation guide](https://docs.dagster.io/getting-started/installation) for information.

## Step 1: Set up the Dagster environment [​](https://docs.dagster.io/getting-started/quickstart\#step-1-set-up-the-dagster-environment "Direct link to Step 1: Set up the Dagster environment")

1. Open the terminal and create a new directory for your project:





```codeBlockLines_e6Vv
mkdir dagster-quickstart
cd dagster-quickstart

```

2. Create and activate a virtual environment:



- MacOS
- Windows

`bash python -m venv venv source venv/bin/activate `

`bash python -m venv venv source venv\Scripts\activate `

3. Install Dagster and the required dependencies:





```codeBlockLines_e6Vv
pip install dagster dagster-webserver pandas

```


## Step 2: Create the Dagster project structure [​](https://docs.dagster.io/getting-started/quickstart\#step-2-create-the-dagster-project-structure "Direct link to Step 2: Create the Dagster project structure")

info

The project structure in this guide is simplified to allow you to get started quickly. When creating new projects, use `dagster project scaffold` to generate a complete Dagster project.

Next, you'll create a basic Dagster project that looks like this:

```codeBlockLines_e6Vv
dagster-quickstart/
├── quickstart/
│   ├── __init__.py
│   └── assets.py
├── data/
    └── sample_data.csv

```

1. To create the files and directories outlined above, run the following:





```codeBlockLines_e6Vv
mkdir quickstart data
touch quickstart/__init__.py quickstart/assets.py
touch data/sample_data.csv

```

2. In the `data/sample_data.csv` file, add the following content:





```codeBlockLines_e6Vv
id,name,age,city
1,Alice,28,New York
2,Bob,35,San Francisco
3,Charlie,42,Chicago
4,Diana,31,Los Angeles

```









This CSV will act as the data source for your Dagster pipeline.


## Step 3: Define the assets [​](https://docs.dagster.io/getting-started/quickstart\#step-3-define-the-assets "Direct link to Step 3: Define the assets")

Now, create the assets for the ETL pipeline. Open `quickstart/assets.py` and add the following code:

```codeBlockLines_e6Vv
import pandas as pd

import dagster as dg

@dg.asset
def processed_data():
    ## Read data from the CSV
    df = pd.read_csv("data/sample_data.csv")

    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv("data/processed_data.csv", index=False)
    return "Data loaded successfully"

## Tell Dagster about the assets that make up the pipeline by
## passing it to the Definitions object
## This allows Dagster to manage the assets' execution and dependencies
defs = dg.Definitions(assets=[processed_data])

```

This may seem unusual if you're used to task-based orchestration. In that case, you'd have three separate steps for extracting, transforming, and loading.

However, in Dagster, you'll model your pipelines using assets as the fundamental building block, rather than tasks.

## Step 4: Run the pipeline [​](https://docs.dagster.io/getting-started/quickstart\#step-4-run-the-pipeline "Direct link to Step 4: Run the pipeline")

1. In the terminal, navigate to your project's root directory and run:





```codeBlockLines_e6Vv
dagster dev -f quickstart/assets.py

```

2. Open your web browser and navigate to `http://localhost:3000`, where you should see the Dagster UI:

![2048 resolution](https://docs.dagster.io/assets/images/dagster-ui-start-d9e349c934143f3dd6436f961f049c0b.png)

3. In the top navigation, click **Assets > View global asset lineage**.

4. Click **Materialize** to run the pipeline.

5. In the popup that displays, click **View**. This will open the **Run details** page, allowing you to view the run as it executes.

![2048 resolution](https://docs.dagster.io/assets/images/run-details-afce3b782860e6c3d60c9a61cb7ebbeb.png)

Use the **view buttons** in near the top left corner of the page to change how the run is displayed. You can also click the asset to view logs and metadata.


## Step 5: Verify the results [​](https://docs.dagster.io/getting-started/quickstart\#step-5-verify-the-results "Direct link to Step 5: Verify the results")

In your terminal, run:

```codeBlockLines_e6Vv
cat data/processed_data.csv

```

You should see the transformed data, including the new `age_group` column:

```codeBlockLines_e6Vv
id,name,age,city,age_group
1,Alice,28,New York,Young
2,Bob,35,San Francisco,Middle
3,Charlie,42,Chicago,Senior
4,Diana,31,Los Angeles,Middle

```

## Next steps [​](https://docs.dagster.io/getting-started/quickstart\#next-steps "Direct link to Next steps")

Congratulations! You've just built and run your first pipeline with Dagster. Next, you can:

- Continue with the [ETL pipeline tutorial](https://docs.dagster.io/etl-pipeline-tutorial/) to learn how to build a more complex ETL pipeline
- Learn how to [Think in assets](https://docs.dagster.io/guides/build/assets/)

- [What you'll learn](https://docs.dagster.io/getting-started/quickstart#what-youll-learn)
- [Prerequisites](https://docs.dagster.io/getting-started/quickstart#prerequisites)
- [Step 1: Set up the Dagster environment](https://docs.dagster.io/getting-started/quickstart#step-1-set-up-the-dagster-environment)
- [Step 2: Create the Dagster project structure](https://docs.dagster.io/getting-started/quickstart#step-2-create-the-dagster-project-structure)
- [Step 3: Define the assets](https://docs.dagster.io/getting-started/quickstart#step-3-define-the-assets)
- [Step 4: Run the pipeline](https://docs.dagster.io/getting-started/quickstart#step-4-run-the-pipeline)
- [Step 5: Verify the results](https://docs.dagster.io/getting-started/quickstart#step-5-verify-the-results)
- [Next steps](https://docs.dagster.io/getting-started/quickstart#next-steps)

```

### File: curated_dagster_docs/foundation/core/io_managers_resources_basics/external-resources.md
```markdown

On this page

Dagster resources are objects used by Dagster assets and ops that provide access to external systems, databases, or services. For example, a simple ETL (Extract Transform Load) pipeline fetches data from an API, ingests it into a database, and updates a dashboard. External tools and services this pipeline uses could be:

- The API the data is fetched from
- The AWS S3 bucket where the API's response is stored
- The Snowflake/Databricks/BigQuery account the data is ingested into
- The BI tool the dashboard was made in

Using Dagster resources, you can standardize connections and integrations to these tools across Dagster definitions like [asset definitions](https://docs.dagster.io/guides/build/assets), [schedules](https://docs.dagster.io/guides/automate/schedules), [sensors](https://docs.dagster.io/guides/automate/sensors), and [jobs](https://docs.dagster.io/guides/build/jobs/).

Resources allow you to:

- **Plug in different implementations in different environments** \- If you have a heavy external dependency that you want to use in production but avoid using in testing, you can accomplish this by providing different resources in each environment.
- **Surface configuration in the Dagster UI** \- Resources and their configuration are surfaced in the UI, making it easy to see where resources are used and how they're configured.
- **Share configuration across multiple assets or ops** \- Resources are configurable and shared, so configuration can be supplied in one place instead of individually.
- **Share implementations across multiple assets or ops** \- When multiple assets access the same external services, resources provide a standard way to structure your code to share the implementations.

## Relevant APIs [​](https://docs.dagster.io/guides/build/external-resources\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) | The base class extended to define resources. Under the hood, implements [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). |
| `ResourceParam` | An annotation used to specify that a plain Python object parameter for an asset or op is a resource. |
| [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) | Class for resource definitions. You almost never want to use initialize this class directly. Instead, you should extend the [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) class which implements [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). |
| [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) | The context object provided to a resource during initialization. This object contains required resources, config, and other run information. |
| [`build_init_resource_context`](https://docs.dagster.io/api/dagster/resources#dagster.build_init_resource_context) | Function for building an [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) outside of execution, intended to be used when testing a resource. |
| [`build_resources`](https://docs.dagster.io/api/dagster/resources#dagster.build_resources) | Function for initializing a set of resources outside of the context of a job's execution. |
| [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources) | Advanced API for providing resources to a specific set of asset definitions, overriding those provided to [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). |

- [Relevant APIs](https://docs.dagster.io/guides/build/external-resources#relevant-apis)

```

### File: curated_dagster_docs/foundation/core/io_managers_resources_basics/io-managers.md
```markdown

On this page

I/O managers in Dagster allow you to keep the code for data processing separate from the code for reading and writing data. This reduces repetitive code and makes it easier to change where your data is stored.

In many Dagster pipelines, assets can be broken down as the following steps:

1. Reading data a some data store into memory
2. Applying in-memory transform
3. Writing the transformed data to a data store

For assets that follow this pattern, an I/O manager can streamline the code that handles reading and writing data to and from a source.

note

This article assumes familiarity with: [assets](https://docs.dagster.io/guides/build/assets/) and [resources](https://docs.dagster.io/guides/build/external-resources/)

## Before you begin [​](https://docs.dagster.io/guides/build/io-managers\#before-you-begin "Direct link to Before you begin")

**I/O managers aren't required to use Dagster, nor are they the best option in all scenarios.** If you find yourself writing the same code at the start and end of each asset to load and store data, an I/O manager may be useful. For example:

- You have assets that are stored in the same location and follow a consistent set of rules to determine the storage path
- You have assets that are stored differently in local, staging, and production environments
- You have assets that load upstream dependencies into memory to do the computation

**I/O managers may not be the best fit if:**

- You want to run SQL queries that create or update a table in a database
- Your pipeline manages I/O on its own by using other libraries/tools that write to storage
- Your assets won't fit in memory, such as a database table with billions of rows

As a general rule, if your pipeline becomes more complicated in order to use I/O managers, it's likely that I/O managers aren't a good fit. In these cases you should use `deps` to [define dependencies](https://docs.dagster.io/guides/build/assets/passing-data-between-assets).

## Using I/O managers in assets [​](https://docs.dagster.io/guides/build/io-managers\#io-in-assets "Direct link to Using I/O managers in assets")

Consider the following example, which contains assets that construct a DuckDB connection object, read data from an upstream table, apply some in-memory transform, and write the result to a new table in DuckDB:

```codeBlockLines_e6Vv
import pandas as pd
from dagster_duckdb import DuckDBResource

import dagster as dg

raw_sales_data = dg.AssetSpec("raw_sales_data")

@dg.asset
def raw_sales_data(duckdb: DuckDBResource) -> None:
    # Read data from a CSV
    raw_df = pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Use the data from the CSV to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_sales_data AS SELECT * FROM raw_df"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO raw_sales_data SELECT * FROM raw_df")

# Asset dependent on `raw_sales_data` asset
@dg.asset(deps=[raw_sales_data])
def clean_sales_data(duckdb: DuckDBResource) -> None:
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Select data from table
        df = conn.execute("SELECT * FROM raw_sales_data").fetch_df()

        # Apply transform
        clean_df = df.fillna({"amount": 0.0})

        # Use transformed result to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clean_sales_data AS SELECT * FROM clean_df"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO clean_sales_data SELECT * FROM clean_df")

# Asset dependent on `clean_sales_data` asset
@dg.asset(deps=[clean_sales_data])
def sales_summary(duckdb: DuckDBResource) -> None:
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Select data from table
        df = conn.execute("SELECT * FROM clean_sales_data").fetch_df()

        # Apply transform
        summary = df.groupby(["owner"])["amount"].sum().reset_index()

        # Use transformed result to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sales_summary AS SELECT * from summary"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO sales_summary SELECT * from summary")

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    resources={"duckdb": DuckDBResource(database="sales.duckdb", schema="public")},
)

```

Using an I/O manager would remove the code that reads and writes data from the assets themselves, instead delegating it to the I/O manager. The assets would be left only with the code that applies transformations or retrieves the initial CSV file.

```codeBlockLines_e6Vv
import pandas as pd
from dagster_duckdb_pandas import DuckDBPandasIOManager

import dagster as dg

@dg.asset
def raw_sales_data() -> pd.DataFrame:
    return pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")

@dg.asset
# Load the upstream `raw_sales_data` asset as input & specify the returned data type (`pd.DataFrame`)
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    # Storing data with an I/O manager requires returning the data
    return raw_sales_data.fillna({"amount": 0.0})

@dg.asset
def sales_summary(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    return clean_sales_data.groupby(["owner"])["amount"].sum().reset_index()

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    # Define the I/O manager and pass it to `Definitions`
    resources={
        "io_manager": DuckDBPandasIOManager(database="sales.duckdb", schema="public")
    },
)

```

To load upstream assets using an I/O manager, specify the asset as an input parameter to the asset function. In this example, the `DuckDBPandasIOManager` I/O manager will read the DuckDB table with the same name as the upstream asset ( `raw_sales_data`) and pass the data to `clean_sales_data` as a Pandas DataFrame.

To store data using an I/O manager, return the data in the asset function. The returned data must be a valid type. This example uses Pandas DataFrames, which the `DuckDBPandasIOManager` will write to a DuckDB table with the same name as the asset.

Refer to the individual I/O manager documentation for details on valid types and how they store data.

## Swapping data stores [​](https://docs.dagster.io/guides/build/io-managers\#swap-data-stores "Direct link to Swapping data stores")

With I/O managers, swapping data stores consists of changing the implementation of the I/O manager. The asset definitions, which only contain transformational logic, won't need to change.

In the following example, a Snowflake I/O manager replaced the DuckDB I/O manager.

```codeBlockLines_e6Vv
import pandas as pd
from dagster_snowflake_pandas import SnowflakePandasIOManager

import dagster as dg

@dg.asset
def raw_sales_data() -> pd.DataFrame:
    return pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")

@dg.asset
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    return raw_sales_data.fillna({"amount": 0.0})

@dg.asset
def sales_summary(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    return clean_sales_data.groupby(["owner"])["amount"].sum().reset_index()

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    resources={
        # Swap in a Snowflake I/O manager
        "io_manager": SnowflakePandasIOManager(
            database=dg.EnvVar("SNOWFLAKE_DATABASE"),
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("SNOWFLAKE_USER"),
            password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
        )
    },
)

```
