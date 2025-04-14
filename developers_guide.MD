# SQLMESH Developers guide

This command is really useful

```
sqlmesh --help
```

## Checkout dev

```
git checkout sandbox
```

## Do some local development

Make some changes, then run:
```
sqlmesh plan dev
```
or (w filter)
```
sqlmesh plan --select-model "clockwork_sll*" dev
```

## Production

If the plan can be executed, you may promote it to production

```
sqlmesh plan prod
```
or (w filter)
```
sqlmesh plan --select-model "clockwork_sll*" prod
```
