import scifor

pi_assessment = scifor.PathInput("{subject}/{session}/gaitrite.csv", root_folder="root/assessment")
pi_training = scifor.PathInput("{subject}/{session}/gaitrite.csv", root_folder="root/training")

result = scifor.for_each(
    lambda filepath: str(filepath),
    inputs={"filepath": scifor.EachOf(pi_assessment, pi_training)},
    dry_run=True,
    subject=[], session=[]
)

# scifor.Fixed(my_var, session="BL")

# def compute_delta_from_bl(session_val: float, bl_val: float) -> float:
#     return session_val - bl_val


# result = scifor.for_each(compute_delta_from_bl
#     inputs={
#         "session_val": session_val, "bl_val": scifor.Fixed(bl_val, session="BL")
#     },
#     outputs=["delta_from_bl"],
#     subject= [], session = []
# )