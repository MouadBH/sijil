import re
import uuid

from sijil_core.tenancy import tenant_db_names, tenant_ref

_IDENTIFIER = re.compile(r"^[a-z0-9_]{1,63}$")


def test_ref_is_short_opaque_and_deterministic() -> None:
    workspace_id = uuid.uuid4()
    ref = tenant_ref(workspace_id)
    assert re.fullmatch(r"t[a-z2-7]{16}", ref)
    assert tenant_ref(workspace_id) == ref
    assert tenant_ref(uuid.uuid4()) != ref


def test_db_names_follow_the_ref() -> None:
    ref = tenant_ref(uuid.uuid4())
    db_name, index_db_name = tenant_db_names(ref)
    assert (db_name, index_db_name) == (f"paperless_{ref}", f"index_{ref}")
    assert _IDENTIFIER.fullmatch(db_name)
    assert _IDENTIFIER.fullmatch(index_db_name)
