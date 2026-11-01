from sijil_core.tenancy import tenant_db_names


def test_db_names_follow_the_slug() -> None:
    assert tenant_db_names("atlas-sarl-ab12cd") == (
        "paperless_atlas_sarl_ab12cd",
        "index_atlas_sarl_ab12cd",
    )
