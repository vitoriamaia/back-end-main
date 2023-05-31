from controller import db
from model.anonymization_type_model import (
    AnonymizationType,
    anonymization_types_share_schema,
)
from model.user_model import User


def get_anonymizations_type() -> tuple[list[dict], int]:
    """
    This function returns the registered anonymization types.

    Parameters:
    ----------
        No parameters

    Returns:
    -------
    tuple[list[dict], int]
        (registered anonymization types, status code).
    """

    registered_anonymization_types = anonymization_types_share_schema.dump(
        AnonymizationType.query.all()
    )

    return (registered_anonymization_types, 200)


def add_anonymization_type(
    current_user: User, anonymization_type_name: str
) -> tuple[int, str]:
    """
    This function adds a anonymization type.

    Parameters:
    ----------
    current_user : User
        Object representing the current user.

    anonymization_type_name : str
        Anonymization type name.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    if current_user.is_admin != 1:
        return (403, "required_administrator_privileges")

    anonymization_type = AnonymizationType(name=anonymization_type_name)

    db.session.add(anonymization_type)
    db.session.commit()

    return (200, "anonymization_type_added")


def delete_anonymization_type(
    current_user: User, id_anonymization_type: int, anonymization_type_name: str
) -> tuple[int, str]:
    """
    This function deletes a registered anonymization type.

    Parameters:
    ----------
    current_user : User
        Object representing the current user.

    id_anonymization_type : int
        Anonymization type ID.

    anonymization_type_name : str
        Anonymization type name.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    if current_user.is_admin != 1:
        return (403, "required_administrator_privileges")

    # Search anonymization type by id or name
    if id_anonymization_type:
        anonymization_type = AnonymizationType.query.filter_by(
            id=id_anonymization_type
        ).first()

    else:
        anonymization_type = AnonymizationType.query.filter_by(
            name=anonymization_type_name
        ).first()

    if anonymization_type == None:
        return (404, "anonymization_type_not_found")

    db.session.delete(anonymization_type)
    db.session.commit()

    return (404, "anonymization_type_deleted")
