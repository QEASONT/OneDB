from py4j.java_gateway import JavaGateway, GatewayParameters


class SparkClient:
    def __init__(self):
        pass

    def say_hello(server_ip, port):
        gateway = JavaGateway(gateway_parameters=GatewayParameters(address="server_ip"))
        hello_app = gateway.entry_point
        value = hello_app.sayHello("hi")
        return value


def main():
    gateway = JavaGateway(gateway_parameters=GatewayParameters(address="10.214.131.7"))
    addition_app = gateway.entry_point
    value = addition_app.runSpark(100, 10000, 200, 200, 0.5)

    print(value)


if __name__ == "__main__":
    main()
