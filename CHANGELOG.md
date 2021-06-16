# Changelog

## Intake-thredds v2021.6.16

([full changelog](https://github.com/NCAR/intake-thredds/compare/4fb261a1f131cefd1083a66b7a10fe1a30eaaaa8...ff4d2720b95077254c0eb184c9ddfe0b3dfc7ca0))

### Enhancements made

- Add tqdm to requirements [#41](https://github.com/NCAR/intake-thredds/pull/41) ([@raybellwaves](https://github.com/raybellwaves))
- Allow `xarray_kwargs` for {py:class}`~intake_thredds.source.THREDDSMergedSource` [#32](https://github.com/NCAR/intake-thredds/pull/32) ([@aaronspring](https://github.com/aaronspring))
- Allow `concat_kwargs` for {py:class}`~intake_thredds.source.THREDDSMergedSource` [#34](https://github.com/NCAR/intake-thredds/pull/34) ([@aaronspring](https://github.com/aaronspring))

### Maintenance and upkeep improvements

- Bump pre-commit/action from v2.0.2 to v2.0.3 [#31](https://github.com/NCAR/intake-thredds/pull/31) ([@dependabot](https://github.com/dependabot))
- Bump styfle/cancel-workflow-action from 0.8.0 to 0.9.0 [#29](https://github.com/NCAR/intake-thredds/pull/29) ([@dependabot](https://github.com/dependabot))
- Bump pre-commit/action from v2.0.0 to v2.0.2 [#28](https://github.com/NCAR/intake-thredds/pull/28) ([@dependabot](https://github.com/dependabot))

### Documentation improvements

- DOC: GEFS example [#40](https://github.com/NCAR/intake-thredds/pull/40) ([@raybellwaves](https://github.com/raybellwaves))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/NCAR/intake-thredds/graphs/contributors?from=2021-02-17&to=2021-06-16&type=c))

[@aaronspring](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Aaaronspring+updated%3A2021-02-17..2021-06-16&type=Issues) | [@andersy005](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Aandersy005+updated%3A2021-02-17..2021-06-16&type=Issues) | [@dependabot](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Adependabot+updated%3A2021-02-17..2021-06-16&type=Issues) | [@raybellwaves](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Araybellwaves+updated%3A2021-02-17..2021-06-16&type=Issues)

## Intake-thredds v2021.2.17

([full changelog](https://github.com/NCAR/intake-thredds/compare/792fdc08e7fbbf66455fe554ca9a0f1f8a14ae32...ccb3c469a07cc7adf058ce0f8ba41197ebc5b7c7))

### Features

- Turn off notebook execution [#22](https://github.com/NCAR/intake-thredds/pull/22) ([@andersy005](https://github.com/andersy005))
- Add driver and simplecache example [#20](https://github.com/NCAR/intake-thredds/pull/20) ([@aaronspring](https://github.com/aaronspring))
- Add documentation [#19](https://github.com/NCAR/intake-thredds/pull/19) ([@andersy005](https://github.com/andersy005))
- 💚 Update Continuous Integration [#18](https://github.com/NCAR/intake-thredds/pull/18) ([@andersy005](https://github.com/andersy005))
- Add LICENSE [#17](https://github.com/NCAR/intake-thredds/pull/17) ([@andersy005](https://github.com/andersy005))
- Docs: Use markdown via myst-parser [#16](https://github.com/NCAR/intake-thredds/pull/16) ([@andersy005](https://github.com/andersy005))
- Allow simplecache [#14](https://github.com/NCAR/intake-thredds/pull/14) ([@aaronspring](https://github.com/aaronspring))
- options for combine_by_coords [#13](https://github.com/NCAR/intake-thredds/pull/13) ([@aaronspring](https://github.com/aaronspring))
- Pass kwargs to Catalog [#12](https://github.com/NCAR/intake-thredds/pull/12) ([@martindurant](https://github.com/martindurant))
- 💚 Migrate CI from CircleCI to GHA [#10](https://github.com/NCAR/intake-thredds/pull/10) ([@andersy005](https://github.com/andersy005))
- Use opendap [#7](https://github.com/NCAR/intake-thredds/pull/7) ([@aaronspring](https://github.com/aaronspring))
- Add tests [#5](https://github.com/NCAR/intake-thredds/pull/5) ([@andersy005](https://github.com/andersy005))
- CI & RTD configurations Overhaul [#4](https://github.com/NCAR/intake-thredds/pull/4) ([@andersy005](https://github.com/andersy005))
- Add `THREDDSMergedSource` implementation [#3](https://github.com/NCAR/intake-thredds/pull/3) ([@martindurant](https://github.com/martindurant))
- Add example [#2](https://github.com/NCAR/intake-thredds/pull/2) ([@martindurant](https://github.com/martindurant))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/NCAR/intake-thredds/graphs/contributors?from=2019-01-05&to=2021-02-18&type=c))

[@aaronspring](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Aaaronspring+updated%3A2019-01-05..2021-02-18&type=Issues) | [@andersy005](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Aandersy005+updated%3A2019-01-05..2021-02-18&type=Issues) | [@dopplershift](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Adopplershift+updated%3A2019-01-05..2021-02-18&type=Issues) | [@martindurant](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Amartindurant+updated%3A2019-01-05..2021-02-18&type=Issues) | [@pbranson](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Apbranson+updated%3A2019-01-05..2021-02-18&type=Issues) | [@rabernat](https://github.com/search?q=repo%3ANCAR%2Fintake-thredds+involves%3Arabernat+updated%3A2019-01-05..2021-02-18&type=Issues)
