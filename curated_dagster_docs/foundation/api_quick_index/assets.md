
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
