# Key Changes to shovel_validators

## Summary
The main change is reversing the logic from "finding children of validators with permits" to "finding parents of registered validators". This correctly identifies parent validators who have access to subnets through their child keys.

## Main Changes:

### 1. Updated `decode_account_id` function
```python
def decode_account_id(account_id_bytes: Union[tuple[int], tuple[tuple[int]]]):
    # Handle BittensorScaleType objects
    if hasattr(account_id_bytes, 'value'):
        account_id_bytes = account_id_bytes.value
    
    # Handle nested tuples
    if isinstance(account_id_bytes, tuple) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], (tuple, list)):
        account_id_bytes = account_id_bytes[0]
    
    # Handle list of tuples
    if isinstance(account_id_bytes, list) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], tuple):
        account_id_bytes = account_id_bytes[0]
        
    return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)
```

### 2. New function: `get_registered_validators_in_subnet`
Gets all validators registered in a specific subnet by querying the Keys storage map.

### 3. New function: `get_parent_keys_for_validator`
For a given validator (child), finds all its parent validators using the ParentKeys storage map.

### 4. Completely new `fetch_all_validators_data_new` function
The new logic:
1. For each subnet, get all registered validators
2. For each registered validator, check if they have parents (making them child keys)
3. Track which parents have access to which subnets through their children
4. Build validator data that includes both direct registrations and child-accessible subnets

### 5. Updated registrations logic
```python
# Calculate registrations (direct registrations + child access)
registrations = list(val_data["subnets"])
if address in parents_with_child_access:
    # Add subnets accessible via children
    child_access_subnets = list(parents_with_child_access[address])
    # Merge with direct registrations
    registrations = list(set(registrations + child_access_subnets))
```

### 6. Data structure changes
- Validators now track both direct subnet registrations and child-accessible subnets
- The `registrations` field in the output now includes ALL subnets a validator can access (directly or through children)
- Parent validators will show up with registrations that include their children's subnets

## Key Benefits:
1. **Correct parent-child relationships**: Parents are properly identified through their children
2. **Accurate subnet access**: Parents show all subnets they can access, including through child keys
3. **Better performance**: Only queries registered validators instead of all validators with permits
4. **Maintains compatibility**: Output format remains the same, only the logic changes

## Example:
If Opentensor Foundation (5G3wMP3...) has a child key registered in subnet 3, the parent will now show:
- `registrations`: [0, 3] (subnet 0 from permits, subnet 3 from child access)
- This correctly reflects that the parent can validate in subnet 3 through its child