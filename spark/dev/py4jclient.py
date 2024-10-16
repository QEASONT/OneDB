from py4j.java_gateway import JavaGateway, GatewayParameters


def main():
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(address="10.214.131.7")
    )
    # random = gateway.jvm.java.util.Random()  # create a java.util.Random instance
    addition_app = gateway.entry_point
    value = addition_app.runSpark(10**7, 10, 5, 5, 20, 20)
    print(value)


if __name__ == "__main__":
    main()
