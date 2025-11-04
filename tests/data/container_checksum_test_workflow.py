from highway_dsl import WorkflowBuilder

def demonstrate_container_checksum_workflow():
    """
    Defines a complex workflow that tests container execution and checksum validation.
    """

    builder = WorkflowBuilder("Tricky Checksum Validator")

    # 1. (Python) Set the multiplier in memory. This is the root dependency.
    builder.task(
        "set_multiplier",
        "tools.memory.set",
        args=["multiplier", 7],  # This is our secret multiplier
        result_key="set_ok",
    )

    # --- ALPHA BRANCH (Parallel) ---
    
    # 2. (Docker) Calculate (Base * 2). Depends on multiplier being set.
    builder.task(
        "calc_alpha_part1",
        "tools.command.run",
        args=[
            "docker", "run", "--rm", "alpine:latest",
            "/bin/sh", "-c", "echo $(({{ variables.base }} * 2))"
        ],
        dependencies=["set_multiplier"],
        result_key="alpha_p1",
    )

    # 3. (Docker) Calculate (Multiplier * 3). Depends on multiplier being set.
    builder.task(
        "calc_alpha_part2",
        "tools.command.run",
        args=[
            "docker", "run", "--rm", "busybox:latest",
            "/bin/sh", "-c", "echo $(({{ memory.multiplier }} * 3))"
        ],
        dependencies=["set_multiplier"],
        result_key="alpha_p2",
    )

    # --- BETA BRANCH (Parallel) ---
    
    # 4. (Docker) Calculate (Base / 4). Depends on multiplier being set.
    builder.task(
        "calc_beta_part1",
        "tools.command.run",
        args=[
            "docker", "run", "--rm", "alpine:latest",
            "/bin/sh", "-c", "echo $(({{ variables.base }} / 4))"
        ],
        dependencies=["set_multiplier"],
        result_key="beta_p1",
    )

    # --- AGGREGATION (FAN-IN) ---
    
    # 5. (Python) Aggregate the Alpha branch.
    builder.task(
        "aggregate_alpha",
        "tools.command.run",
        args=[
            "python3", "-c",
            "print(int(float('{{ results.alpha_p1 }}'.strip())) + int(float('{{ results.alpha_p2 }}'.strip())))"
        ],
        dependencies=["calc_alpha_part1", "calc_alpha_part2"],
        result_key="alpha_final",
    )

    # 6. (Python) Aggregate the Beta branch.
    builder.task(
        "aggregate_beta",
        "tools.command.run",
        args=[
            "python3", "-c",
            "print(int(float('{{ results.beta_p1 }}'.strip())) + 1)"
        ],
        dependencies=["calc_beta_part1"],
        result_key="beta_final",
    )

    # --- FINAL CALCULATION ---
    
    # 7. (Python) Multiply the two branch results.
    builder.task(
        "final_calculation",
        "tools.command.run",
        args=[
            "python3", "-c",
            "print(int(float('{{ results.alpha_final }}'.strip())) * int(float('{{ results.beta_final }}'.strip())))"
        ],
        dependencies=["aggregate_alpha", "aggregate_beta"],
        result_key="the_final_number",
    )

    workflow = builder.build()

    workflow.set_variables({
        "base": 100,
    })

    return workflow


if __name__ == "__main__":
    container_workflow = demonstrate_container_checksum_workflow()
    print(container_workflow.to_yaml())