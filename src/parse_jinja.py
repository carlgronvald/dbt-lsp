from jinja2 import Environment
env = Environment()
temp = env.from_string("Hello {{ name }}")
temp.render()