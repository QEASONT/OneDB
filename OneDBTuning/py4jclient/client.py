from py4j.java_gateway import JavaGateway, GatewayParameters


class SparkClient:
    def __init__(self):
        pass

    def say_hello(server_ip, port):
        gateway = JavaGateway(gateway_parameters=GatewayParameters(address="server_ip"))
        hello_app = gateway.entry_point
        value = hello_app.sayHello("qt")
        return value


def main():
    gateway = JavaGateway(gateway_parameters=GatewayParameters(address="10.214.131.7"))
    addition_app = gateway.entry_point
    value = addition_app.runSpark(10**7, 10, 5, 5, 20, 20)
    print(value)


if __name__ == "__main__":
    main()
