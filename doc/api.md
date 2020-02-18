# EggRoll API Document

* 1. **[Introduction] (#1)**
* 2. **[Session] (#2)**
* 3. **[RollPair] (#3)**
    * 3.1. **[RollPairContext] (#3.1)** 
    * 3.2. **[Storage] (#3.2)**
    * 3.3. **[Computing] (#3.3)** 
* 4. **[RollSite] (#4)**
    * 4.1. **[RollSiteContext] (#4.1)** 
    * 4.2. **[Long Distance Communication] (#4.2)** 


---

## <a name="1">1. Introduction</a>

## <a name="2">2. Session</a>

Eggroll session is defined in class `ErSession`.

>``` python
>__init__(session_id=f'er_session_py_{time_in_'%Y%m%d.%H%M%S.%f'}', name='', tag='', processors=list(), options=dict())
>```

Creates a Eggroll session.

**Parameters:**

+ **session\_id** (str): Session id and default table namespace of this runtime.
+ **name** (str): Session name.
+ **tag** (str): Session tag.
+ **processors** (list): List of session processors. If it is an empty list, processors are created by the cluster manager. Else these processors will be registered.
+ **options** (dict): Dictionary to store other parameters.

**Returns:**

+ None

**Example:**

``` python
>>> from eggroll.core.session import ErSession
>>> session = ErSession()
```

--

>``` python
>route_to_egg(partition: ErPartition)
> ```

Routes the access of `ErPartition` to an egg.

**Parameters:**

+ **partition** (ErPartition): The partition needs to be routed.


**Returns:**

+ **egg** (ErProcessor): The egg to which the partition is bound.

**Example:**

``` python
>>> egg = session.route_to_egg(partition)
```

--

>``` python
>stop()
>```

Stops the existing session.

**Parameters:**

+ **None**

**Returns:**

+ **session_meta** (ErSessionMeta): Metadata of the stopped session.

**Example:**

``` python
>>> session.stop()
```

--

>``` python
>set_rp_recorder(roll_pair_context: RollPairContext)
>```

Sets garbage recorder of a context.

**Parameters:**

+ **roll_pair_context** (RollPairContext): The RollPairContext needs gc.

**Returns:**

+ None

**Example:**

``` python
>>> session.set_rp_recorder(rpc)
```

--

>`get_session_id()`

Gets session id of the session.

**Parameters:**

+ None

**Returns:**

+ **session_id** (str).

**Example:**

``` python
>>> session.get_session_id()
'er_session_py_20200101.101010.999_192.168.0.1'
```

--

>``` python
>get_session_meta()
>```

Gets the metadata / configs of the session.

**Parameters:**

+ None

**Returns:**

+ **session_meta** (ErSessionMeta)

**Example:**

``` python
>>> session.get_session_meta()
```

--

>`add_cleanup_task(func)`

Adds a cleanup task to this session. Cleanup task will be run when session completes.

**Parameters:**

+ **func** (function): Function to be run.

**Returns:**

+ None 

**Example:**

``` python
>>> session.add_cleanup_task(lambda : print('hello'))
```

--

>`run_cleanup_task(eggroll)`

Runs cleanup tasks added to this session. This method will be run when session stops.

**Parameters:**

+ None

**Returns:**

+ None 

**Example:**

``` python
>>> session.run_cleanup_tasks()
```

--

>`get_option(self, key, default=None)`

Gets a session option by key.

**Parameters:**

+ **key** (str): The key of the session option.
+ **default** (str): The default value of the key is not found in session's option.

**Returns:**

+ **value** (str): The value of the option. 

**Example:**

``` python
>>> session.get_option('eggroll.logs.dir')
'/data/projects/eggroll/logs'
```

--

>`has_option(key)`

Checks if an opiton exists or already be set in session.

**Parameters:**

+ **key** (str): The option key.

**Returns:**

+ **is_exist** (bool): Whether the option exists in session. 

**Example:**

``` python
>>> session.has_option('eggroll.logs.dir')
True
```


--

>`get_all_options()`

Gets all options of the session.

**Parameters:**

+ None

**Returns:**

+ **is_exist** (bool): Whether the option exists in session. 

**Example:**

``` python
>>> session.get_options()
```

--


## <a name="3">3. RollPair</a>
RollPair is a key-value based.

### <a name="3.1">3.1. RollPairContext</a>

>``` python
>__init__(self, session: ErSession)
> ```

Initializes a RollPairContext by from an existing session.

**Parameters:**

+ **session** (ErSession): The session on which the new RollPairContext is based.


**Returns:**

+ roll_pair_context (RollPairContext): A new RollPairContext

**Example:**

``` python
>>> rpc = RollPairContext(session)
```

--

>``` python
>set_store_type(self, store_type: str)
> ```

Sets default store type of the RollPairs created from this RollPairContext.

**Parameters:**

+ **store_type** (str): The default store type. A enum-alike string value defined in class `StoreTypes`.


**Returns:**

+ None

**Example:**

``` python
>>> from eggroll.core.contants import StoreTypes
>>> rpc.set_store_type(StoreTypes.ROLLPAIR_LMDB)
```

--

>``` python
>set_store_serdes(self, serdes_type: str)
> ```

Sets default SerDes type of the RollPairs created from this RollPairContext.

**Parameters:**

+ **serdes_type** (str): The default SerDes type. A enum-alike string value defined in class `SerdesTypes`.


**Returns:**

+ None

**Example:**

``` python
>>> from eggroll.core.contants import SerdesTypes
>>> rpc.set_serdes_type(SerdesTypes.PICKLE)
```

--

>``` python
>set_session_rp_recorder(partition: ErPartition)
> ```

Sets the gc recorder.

**Parameters:**

+ None


**Returns:**

+ None

**Example:**

``` python
>>> rpc.set_session_rp_recorder()
```

--

>``` python
>set_session_gc_enable()
> ```

Enables gc

**Parameters:**

+ None


**Returns:**

+ None

**Example:**

``` python
>>> rpc.set_session_gc_enable()
```

--

>``` python
>set_session_gc_disable()
> ```

Enables gc

**Parameters:**

+ None


**Returns:**

+ None

**Example:**

``` python
>>> rpc.set_session_gc_disable()
```

--

>``` python
>get_session()
> ```

Gets the underlying `ErSession`.

**Parameters:**

+ None


**Returns:**

+ **session** (ErSession): The underlying session of the current context.

**Example:**

``` python
>>> session = rpc.get_session()
```

--

>``` python
>get_roll(self)
> ```

Returns the roll master of the current context. Currently one context has only one roll master.

**Parameters:**

+ None


**Returns:**

+ **roll** (ErProcessor): The RollPairMaster wrapped in `ErProcessor`.

**Example:**

``` python
>>> roll = rpc.get_roll()
```

--

>``` python
>route_to_egg(self, partition: ErPartition)
> ```

Routes the access of `ErPartition` to an egg. A wrapper of `ErSession`'s `route_to_egg`.

**Parameters:**

+ **partition** (ErPartition): The partition needs to be routed.


**Returns:**

+ **egg** (ErProcessor): The egg to which the partition is bound.

**Example:**

``` python
>>> egg = rpc.route_to_egg(partition)
```

--

>``` python
>populate_processor(self, store: ErStore)
> ```

Populates session's processors into the `ErStore`, which consists `ErServerNode`. i.e. binds `ErProcessor` to `ErPartition`.

**Parameters:**

+ **store** (ErStore): The `ErStore` needs `ErProcessor` bindings.


**Returns:**

+ **store** (ErStore): A store with the same `ErStoreLocator` but with `ErProcessor` bindings.

**Example:**

``` python
>>> populated_store = rpc.populate_store(raw_store)
```

--

>``` python
>load(self, namespace: str = None, name: str = None, options = dict())
> ```

Loads a RollPair with `namespace` and `name`, and behaviours controlled by `options`.

**Parameters:**

+ **namespace** (str): The namespace of required RollPair.
+ **name** (str): The name of required RollPair.
+ **options** (dict): Loading behaviours of the RollPair. Current available options:
    + *create\_if\_missing* (bool): `True` creates a new `RollPair` if the required does not exists. `False` otherwise.
    + *total\_partitions* (int): Total partitions of the `RollPair` if a new `RollPair` is created. 


**Returns:**

+ **roll_pair** (RollPair): The loaded `RollPair`.

**Example:**

``` python
>>> rp = rpc.load(namespace='ns', name='n')
```

--

>``` python
>parallelize(self, data, options=dict())
> ```

Returns the roll master of the current context. Currently one context has only one roll master.

**Parameters:**

+ **data** (sequence / generator): Data needs to be parallelized.
+ **options** (dict): Parallelizing behaviours. Current available options:
    + *namespace*: The namespace of the result `RollPair`. None or no-set will use session id.
    + *name*: The name of the result `RollPair`. None or no-set results in a random name based on current time.


**Returns:**

+ **roll_pair** (RollPair): The `RollPair` which stores `data` parallelly.

**Example:**

``` python
>>> rp = rpc.parallelize(range(10), options={"total_partitions": 10})
```


--

### <a name="3.2">3.2. Storage</a>

>`count()`

Returns the number of elements in the `RollPair`.

**Parameters:**

+ None

**Returns:**

+ **num** (int): Number of elements in this DTable.

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> a.count()
10
```

--

>`delete(k)`

Returns the deleted value corresponding to the key.

**Parameters:**

+ **k** (object): Key object. Will be serialized. Must be less than 512 bytes for LMDB.

**Returns:**

+ **v** (object): Corresponding value of the deleted key. Returns None if key does not exist.

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> a.delete(1)
1
```

--

>`destroy()`

Destroys this `RollPair`, freeing its associated storage resources.

**Parameters:**

+ None

**Returns:**

+ None

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> a.destroy()
```

--

>`first(self, options=dict())`

Returning the first element of a `RollPair`. Shortcut of `take(1, keysOnly)`

**Parameters:**

+ **keysOnly** (boolean): Whether to return keys only. `True` returns keys only and `False` returns both keys and values.

**Returns:**

+ **first_element** (tuple / object): First element of the DTable. It is a tuple if `keysOnly=False`, or an object if `keysOnly=True`.

**Example:**

``` python
>>> a = eggroll.parallelize([1, 2, 3])
>>> a.first()
(1, 1)
```

--

>`get(k)`

Fetches the value matching key.

**Parameters:**

+ **k** (object): Key object. Will be serialized. Must be less than 512 bytes.

**Returns:**

+ **v** (object): Corresponding value of the key. Returns None if key does not exist.

**Example:**

``` python
>>> a = eggroll.parallelize(range(10))
>>> a.get(1)
(1, 1)
```

--

>`get_all(self, options=dict())` 

Returns an generator of (key, value) 2-tuple from the `RollPair`. 

**Parameters:**

+ **options** (dict): Reserved for future args.

**Returns:**

+ **generator** (generator): Generator of all data of `RollPair`.

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> b = a.get_all()
>>> list(b)
[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)]
```

--

>`put(k, v)`

Stores a key-value record.

**Parameters:**

+ **k** (object): Key object. Will be serialized. Must be less than 512 bytes if store_type is `ROLLPAIR_LMDB`.
+ **v** (object): Value object. Will be serialized.

**Returns:**

+ None

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> a.put('hello', 'world')
>>> list(a)
[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), ('hello', 'world')]
```

--

>`put_all(self, items, options=dict())`

Puts (key, value) 2-tuple stream from the iterable items. Elements must be exact 2-tuples, they may not be of any other type, or tuple subclass.

**Parameters:**

+ **items** (Iterable): Key-Value 2-tuple iterable. Will be serialized. Each key must be less than 512 bytes, value must be less than 32 MB.
+ **options** (dict): Behaviours of the method. Current available options:
    + *include\_key* (bool): `True` if the item stream contains both key and value, `False` contains value only and the key will be generated.

**Returns:**

+ None

**Example:**

``` python
>>> a = rpc.load('foo', 'bar')
>>> t = [(1, 2), (3, 4), (5, 6)]
>>> a.put_all(t)
>>> list(a.get_all())
[(1, 2), (3, 4), (5, 6)]
```

--

>`save_as(self, name, namespace, partition, options=dict)`

Transforms a temporary table to a persistent table.

**Parameters:**

+ **name** (str): The name of result `RollPair`.
+ **namespace** (str): The namespace of result `RollPair`.
+ **partition** (int): Number of partition for the new persistent table.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): Result `RollPair`.

**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> b = a.save_as('foo', 'bar', partition=2)
```

--

>`take(self, n, options=dict())`

Returns the first n element(s) of a `RollPair`. 

**Parameters:**

+ **n** (int): Number of top data returned.
+ **options** (dict): Behaviours of the method. Current available options:
   + *keys\_only* (bool): `True` returns keys only. `False` returns both keys and values.

**Returns:**

+ **result\_list** (list): Lists of top n keys or key-value pairs.

**Example:**

``` python
>>> a = rpt.parallelize([1, 2, 3])
>>> a.take(2)
[(1, 1), (2, 2)]
>>> a.take(2, options={"keys_only": True})
[1, 2]
```

---

### <a name="3.3">3.3. Computing</a>

>`aggregate(self, zero_value, seq_op, comb_op, output=None, options={})`

Aggregate operation to this `RollPair` using the specified associative binary operator. 


**Parameters:**

+ **zero_value** (object): Zero value of the aggregate operation.
+ **seq_op** (v1, v2 -> v): Binary operator applying to each 2-tuple's value in each partition.
+ **comb_op** (v1, v2 -> v): Binary operator applying to each 2-tuple's value of seq_op result of each partition.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A `RollPair` whose key named `result` contains the result.

**Example:**

``` python
>>> from operator import add, mul
>>> a = rpc.parallelize([1, 2, 3, 4], options={"total_partitions": 2}).aggregate(0, add, mul)
>>> 24

```

--

>`collapse_partitions(func)`

Return a new `RollPair` by applying a function to each partition of this `RollPair`.


**Parameters:**

+ **func** ((k1, v1), (k2, v2) -> (k, v)): The function applying to each partition.

**Returns:**

+ **roll_pair** (RollPair): A new table containing results.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Example:**

``` python
>>> a = eggroll.parallelize([1, 2, 3, 4, 5], partition=2)
>>> def f(iterator):
>>> 	sum = 0
>>> 	for k, v in iterator:
>>> 		sum += v
>>> 	return sum
>>> b = a.collapse_partitions(f)
>>> list(b.get_all())
[(3, 6), (4, 9)]
```

--

>`filter(func, output=None, options=dict())`

Returns a new `RollPair` containing only those keys which satisfy a predicate passed in via `func`.


**Parameters:**

+ **func** (k, v -> boolean): Predicate function returning a boolean.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll\_pair** (RollPair): A new `RollPair` containing results.

**Example:**

``` python
>>> a = rpc.parallelize([0, 1, 2])
>>> b = a.filter(lambda k, v : k % 2 == 0)
>>> list(b.get_all())
[(0, 0), (2, 2)]
>>> c = a.filter(lambda k, v : v % 2 != 0)
>>> list(c.get_all())
[(1, 1)]
```

--

>`flat_map(func, output=None, options{)`

Returns a new DTable by first applying func, then flattening it. Same keys in `func` result in different partitions before shuffle will be overridden, and a random value will be used in the final result.

*This operation may cause shuffle.*


**Parameters:**

+ **func** (k, v -> list): The function applying to each 2-tuple.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll\_pair** (RollPair): A new `RollPair` containing all flattened elements.

**Example:**

``` python
>>> import random
>>> def foo(k, v):
...     result = []
...     r = random.randint(10000, 99999)
...     for i in range(0, k):
...         result.append((k + r + i, v + r + i))
...     return result
>>> a = rpc.parallelize(range(5))
>>> b = a.flat_map(foo)
>>> list(b.get_all())
[(83030, 83030), (84321, 84321), (84322, 84322), (91266, 91266), (91267, 91267), (91268, 91268), (91269, 91269), (71349, 71349), (71350, 71350), (71351, 71351)]
```

--

>`glom(self, output=None, options={})`

Coalescing all elements within each partition into a list.

**Parameters:**

+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll\_pair** (RollPair): A `RollPair` containing all coalesced partition and its elements. First element of each tuple is chosen from key of last element of each partition.

**Example:**

```
>>> a = eggroll.parallelize(range(5), partition=3).glom().collect()
>>> list(a)
[(2, [(2, 2)]), (3, [(0, 0), (3, 3)]), (4, [(1, 1), (4, 4)])]
```

--

>`join(self, other, func, output=None, options={})`

Return an `RollPair` containing all pairs of elements with matching keys in self and other, i.e. 'inner join'.

Each pair of elements will be returned as a (k, func(v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

*This operation may cause shuffle if partitions count of the two operands does not equal.*


**Parameters:**

+ **other** (RollPair): Another `RollPair` to be operated with.
+ **func** (v1, v2 -> v): Binary operator applying to values whose key exists in both DTables.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A `RollPair` containing results.

**Example:**


``` python
>>> a = rpc.parallelize([('a', 1), ('b', 4)], include_key=True)
>>> b = rpc.parallelize([('a', 2), ('c', 3)], include_key=True)
>>> c = a.join(b, lambda v1, v2: v1 + v2)
>>> list(c.get_all())
[('a', 3)]

```

--

>`map(self, func, output=None, options={})`

Return a new `RollPair` by applying a function to each (key, value) 2-tuple of this `RollPair`.

*This operation causes shuffle.*

**Parameters:**

+ **func** (k1, v1 -> k2, v2): The function applying to each 2-tuple.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A new `RollPair` containing results.

**Example:**

``` python
>>> a = rpc.parallelize(['a', 'b', 'c'])    # [(0, 'a'), (1, 'b'), (2, 'c')]
>>> b = a.map(lambda k, v: (v, v + '1'))
>>> list(b.collect())
[("a", "a1"), ("b", "b1"), ("c", "c1")]
```
--

>`map_partitions(self, func, output=None, options={})`

Return a new `RollPair` by applying a function to each partition of this `RollPair`. 


**Parameters:**

+ **func** ((k1, v1), (k2, v2) -> (k, v)): The function applying to each partition.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A new `RollPair` containing results.

**Example:**

``` python
>>> data = [(str(i), i) for i in range(5)]
>>> a = rpt.parallelize(data, include_key=True, partition=2)
>>> def func(iter):
        ret = []
        for k, v in iter:
            ret.append((f"{k}_{v}_0", v ** 2))
            ret.append((f"{k}_{v}_1", v ** 3))
        return ret
>>> b = a.map_partitions(f)
>>> list(b.collect())
[('1_1_0': 1), ('1_1_1': 1), ('2_2_0': 4), ('2_2_1': 8), ('3_3_0': 9), ('3_3_1': 27), ('4_4_0': 16), ('4_4_1': 64), ('5_5_0': 25), ('5_5_1': 125)]
```

--

>`map_values(self, func, output=None, options={})`

Return a `RollPair` by applying a function to each value of this `RollPair`, while keys does not change.


``` python
>>> a = rpc.parallelize([('a', ['apple', 'banana', 'lemon']), ('b', ['grapes'])], include_key=True)
>>> b = a.map_values(lambda x: len(x))
>>> list(b.get_all())
[('a', 3), ('b', 1)]
```

**Parameters:**

+ **func** (v1 -> v2): The function applying to each value.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A new `RollPair` containing results.

**Example:**

``` python
>>> a = rpt.parallelize(['a', 'b', 'c'])    # [(0, 'a'), (1, 'b'), (2, 'c')]
>>> b = a.map_values(lambda v: v + '1')
>>> list(b.get_all())
[(0, 'a1'), (1, 'b1'), (2, 'c1')]
```

--

>`reduce(self, func, output=None, options={})`

Reduces operation to this `RollPair` using the specified associative binary operator. 


**Parameters:**

+ **func** (v1, v2 -> v): Binary operator applying to each 2-tuple's value.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A `RollPair` whose key named `result` contains the result.

**Example:**

``` python
>>> from operator import add
>>> a = rpc.parallelize([1, 2, 3, 4, 5]).reduce(add)
>>> 15

```

--

>`sample(self, fraction, seed=None, output=None, options={})`

Return a sampled subset of this `RollPair`. 


**Parameters:**
 
+ **fraction** (float): Expected size of the sample as a fraction of this DTable's size without replacement: probability that each element is chosen. Fraction must be [0, 1] with replacement: expected number of times each element is chosen.
+ **seed** (float): Seed of the random number generator. Use current timestamp when `None` is passed.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A new `RollPair` containing results.

**Example:**

``` python
>>> x = rpc.parallelize(range(100), partition=4)
>>>  6 <= x.sample(0.1, 81).count() <= 14
True
```

--

>`subtract_by_key(self, other, output=None, options={})`

Returns a new DTable containing elements only in this DTable but not in the other DTable. 

In-place computing applies if enabled. Result will be in left DTable (the caller).

**Parameters:**

+ **other** (RollPair): Another `RollPair` to be operated with.
+ **output** (ErStore): Specified output store of this operation. `None` to create a random store name based on current time.
+ **options** (dict): Behaviours of the method.

**Returns:**

+ **roll_pair** (RollPair): A new `RollPair` containing results.


**Example:**

``` python
>>> a = rpc.parallelize(range(10))
>>> b = rpc.parallelize(range(5))
>>> c = a.subtractByKey(b)
>>> list(c.get_all())
[(5, 5), (6, 6), (7, 7), (8, 8), (9, 9)]
```

--

>`union(self, other, func=lambda v1, v2 : v1, output=None, options={})`

Returns union of this `RollPair` and the other `RollPair`. Function will be applied to values of keys exist in both table.

*This operation may cause shuffle if partitions count of the two operands does not equal.*


**Parameters:**

+ **other** (RollPair): Another `RollPair` to be operated with.
+ **func** (v1, v2 -> v): The function applying to values whose key exists in both DTables. Default using left table's value.

**Returns:**

+ **roll_pair** (RollPair): A `RollPair` containing results.

**Example:**

``` python
>>> a = rpc.parallelize([1, 2, 3])	# [(0, 1), (1, 2), (2, 3)]
>>> b = rpc.parallelize([(1, 1), (2, 2), (3, 3)])
>>> c = a.union(b, lambda v1, v2 : v1 + v2)
>>> list(c.get_all())
[(0, 1), (1, 3), (2, 5), (3, 3)]
```

---

## <a name="4">4. RollSite</a>

### <a name="4.1">4.1. RollSiteContext</a>

>`__init__(self, job_id, self_role, self_partyId, rs_ip, rs_port, rp_ctx)`

Initializes `RollSiteContext` using a bunch of parameters.

**Parameters:**

+ **job\_id** (str): The long range communication job id.
+ **self\_role** (str): The role of myself.
+ **self_partyId** (str): The party id of myself.
+ **rs_ip** (str): The ip address of roll site.
+ **rs_port** (int): The port of roll site
+ **rp_ctx** (RollPairContext): The `RollPairContext` on which this `RollSiteContext` is based.

**Returns:**

+ A new `RollSiteContext`.

**Example:**

``` python
>>> rsc = RollSiteContext(session_id, self_role=role, self_partyId=partyId, rs_ip=host, rs_port=port, rp_ctx=rp_context)
```

--

>`load(self, name, tag)`

Ready for actually push / pull data.

**Parameters:**

+ **name** (str): The consented name among communication parties.
+ **tag** (str): The tag joins with `name` argument to generate a unique name of each communication operation.

**Returns:**

+ A new `RollSite` which follows by push / pull operation.

**Example:**

``` python
>>> rs = rsc.load(name="RsaIntersectTransferVariable.rsa_pubkey", tag="1")
```


### <a name="4.2">4.2. Long Distance Communication</a>


>`__init__(self, name: str, tag: str, rs_ctx: RollSiteContext)`

Creates and initializes a `RollSite` object.

**Parameters:**

+ **name** (str): The consented name among communication parties.
+ **tag** (str): The tag joins with `name` argument to generate a unique name of each communication operation.
+ **rs_ctx** (RollSiteContext): The context on which this `RollSite` is based.


**Returns:**

+ A new `RollSite`.

**Example:**

``` python
>>> rsc = RollSiteContext(session_id, self_role=role, self_partyId=partyId, rs_ip=host, rs_port=port, rp_ctx=rp_context)
>>> rs = RollSite(name="RsaIntersectTransferVariable.rsa_pubkey", tag="1", rs_ctx=rsc)
```

--

>`push(self, obj, parties: list = None)`

Sends data to other parties. This method does not block. A list of `future` will be returned.

**Parameters:**

+ **obj** (object): The object to send. It can be an python `object` or `RollPair`.
+ **parties** (list): A list of parties where the data will be sent.

**Returns:**

+ **future** (list(future)): A list of futures tracking push operations.

**Example:**

``` python
rsc = RollSiteContext(session_id, self_role=role, self_partyId=partyId, rs_ip=host, rs_port=port, rp_ctx=rp_context)
>>> rs = RollSite(name="RsaIntersectTransferVariable.rsa_pubkey", tag="1", rs_ctx=rsc)
>>> a = 'hello'
>>> rs.push(a, ['10000'])
```

--

>`pull(self, parties: list = None)`

Receives data from other parties. This method does not block. A list of `future` will be returned.

**Parameters:**

+ **parties** (list): A list of parties where the data will be sent.

**Returns:**

+ **future** (list(future)): A list of futures tracking push operations.

**Example:**

``` python
rsc = RollSiteContext(session_id, self_role=role, self_partyId=partyId, rs_ip=host, rs_port=port, rp_ctx=rp_context)
>>> rs = RollSite(name="RsaIntersectTransferVariable.rsa_pubkey", tag="1", rs_ctx=rsc)
>>> a =rs.pull(['10000'])
>>> a
>>> 'hello'
```

--


