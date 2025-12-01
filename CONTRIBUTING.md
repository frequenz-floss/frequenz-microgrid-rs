# Contributing to frequenz-microgrid-rs

## Releasing

These are the steps to create a new release:

1. Get the latest head you want to create a release from.

2. Update the version in `Cargo.toml` to the new version.  Without this,
   the new release will be rejected by `crates.io`.

   Along with this, update the `RELEASE_NOTES.md` file if it is not
   complete, up to date, and remove template comments (`<!-- ... ->`)
   and empty sections.

   Submit a pull request if an update is needed, wait until it is
   merged, and update the latest head you want to create a release
   from to get the new merged pull request.

3. Create a new signed tag using the release notes and
   a [semver](https://semver.org/) compatible version number with a `v` prefix,
   for example:

   ```sh
   git tag -s --cleanup=whitespace -F RELEASE_NOTES.md v0.0.1
   ```

4. Push the new tag.

5. A GitHub action will test the tag and if all goes well it will create
   a [GitHub
   Release](https://github.com/frequenz-floss/frequenz-microgrid-rs/releases),
   and upload a new package to
   [crates.io](https://crates.io/crates/frequenz-microgrid)
   automatically.

6. Once this is done, reset the `RELEASE_NOTES.md` with the template:

   ```sh
   cp .github/RELEASE_NOTES.template.md RELEASE_NOTES.md
   ```

   Commit the new release notes and create a PR (this step should be automated
   eventually too).

7. Celebrate!
