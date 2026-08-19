# GitHub upload checklist

## 1. Clean the files

- [ ] Rename `NB_05_GOLD_DIMENSIONAL_MODEL (1).py` to `NB_05_GOLD_DIMENSIONAL_MODEL.py`.
- [ ] Rename `Ecommerce_DAX_Measures_Backup(1).xlsx` to `Ecommerce_DAX_Measures_Backup.xlsx`.
- [ ] Remove the real email from the dynamic RLS mapping.
- [ ] Review notebook outputs for personal or workspace-specific information.
- [ ] Decide whether to publish original notebook exports, cleaned notebook versions, or both.
- [ ] Fix the Sellers and Reviews loading issue described in the runbook.
- [ ] Correct the two suspicious DAX measures before calling the backup final.

## 2. Arrange the repository

- [ ] Put the four notebooks in `notebooks/`.
- [ ] Put the two pipeline exports in `pipelines/`.
- [ ] Put the DAX backup in `semantic-model/`.
- [ ] Put this README and the `docs/` folder at repository root.
- [ ] Create `data/README.md` instead of committing all large raw files, unless redistribution is intentional and allowed.
- [ ] Add a `.gitignore` for local Power BI temporary files, credentials, secrets and operating-system files.

## 3. Add visual evidence

- [x] Successful run of `PL_00_MASTER_ECOMMERCE`.
- [x] Metadata ingestion pipeline with Lookup, ForEach and Copy.
- [x] Lakehouse schemas/tables.
- [x] Workspace lineage view.
- [x] Data-quality results or monitoring page.
- [x] Semantic-model Model view.
- [x] Semantic-model relationship list.
- [x] Dynamic RLS role definition.
- [x] One screenshot for every report page.
- [ ] RLS `Test as role` screenshot with personal identities hidden.

## 4. Review the README

- [x] Replace suggested report-page names with the actual names.
- [x] Add real business findings from the finished dashboard.
- [x] Check that all relative links and image paths work on GitHub.
- [ ] Add the repository owner/contact link only if desired.
- [ ] Add the final project completion date.

## 5. Check portability

- [ ] State clearly that pipeline IDs require rebinding.
- [ ] Confirm that a new workspace can create all nine Bronze tables.
- [ ] Confirm that all 63 DQ rules run.
- [ ] Confirm that all nine Silver and eight core Gold tables are produced.
- [ ] Confirm that the semantic-model refresh runs after Gold.
- [ ] Test the documented setup steps from a clean environment where possible.

## 6. Licensing and data

- [ ] Check the source dataset terms before redistributing the full CSV files.
- [ ] Choose a code licence only after deciding how the project may be reused.
- [ ] Add dataset attribution in `data/README.md`.
- [ ] If the PBIX or another artifact is too large for a normal push, store it using an appropriate large-file method or provide a controlled download link.

## 7. Final quality check

- [ ] No passwords, tokens, private connection strings or personal emails are committed.
- [ ] Python files open and have valid syntax after cleaning.
- [ ] JSON pipeline files are valid JSON.
- [ ] Markdown tables render correctly.
- [ ] Mermaid diagrams render correctly.
- [x] Screenshots do not expose tenant or personal information.
- [ ] The repository description matches what the project actually contains.
