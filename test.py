from docplex.mp.model import Model

model = Model()

x = model.integer_var(lb=0,name="x")

model.add_constraint(
    model.if_then(
        x >= 1,
        x >= 30
    )
)

model.add_constraint(x + 1 <= 50)
model.add_constraint(x - 5 >= 0)
model.solve()
print(model.solution.get_value("x"))