import re


class Prueba:

  def __init__(self):
    self.title_sum_re = re.compile(
        r"##(?P<title>.+)##\n(?P<summary>((?!==.+==).+|\n)+)(?P<rest>(.+|\n)*)"
    )
    self.sections_re = re.compile(r"==.+==\n")
    self.section_re = re.compile(
        r"==(?P<name>.+)==\n(?P<text>((?!--.+--).+|\n)*)(?P<rest>(.+|\n)*)")
    self.subsections_re = re.compile(r"--.+--\n")
    self.subsection_re = re.compile(r"--(?P<name>.+)--\n(?P<text>(.+|\n)*)")

  def obtener_titulo_resumen(self, text: str):

    def clean_text(txt):
      return '\n'.join(l for l in txt.split('\n') if len(l) > 0).strip()

    #Separar título y resumen
    match = self.title_sum_re.match(text)
    dic = None
    if match:
      dic = {}
      dic['title'] = match.group('title')
      dic['summary'] = clean_text(match.group('summary'))
      dic['sections'] = []
      # Separar las secciones
      print(match.group('rest'))
      sec_matches = self.sections_re.finditer(match.group('rest'))
      sec_index = []
      for sec_match in sec_matches:
        sec_index.append(sec_match.span()[0])

      for i in range(len(sec_index)):
        if i == len(sec_index) - 1:
          section = text[sec_index[-1]:-1]

        else:
          section = text[sec_index[i]:sec_index[i + 1]]
        sec_match = self.section_re.match(section)
        if sec_match:
          sec_dic = {}
          sec_dic['name'] = sec_match.group('name')
          sec_dic['text'] = clean_text(sec_match.group('text'))
          sec_dic['subsections'] = []
          subsections = sec_match.group('rest')
          sub_matches = self.subsections_re.finditer(subsections)
          sub_index = []
          for sub_match in sub_matches:
            sub_index.append(sub_match.span()[0])

          for i in range(len(sub_index)):
            if i == len(sub_index) - 1:
              subsection = subsections[sub_index[-1]:-1]
            else:
              subsection = subsections[sub_index[i]:sub_index[i + 1]]
            sub_match = self.subsection_re.match(subsection)
            if sub_match:
              sub_dic = {}
              sub_dic['name'] = sub_match.group('name')
              sub_dic['text'] = clean_text(sub_match.group('text'))
              sec_dic['subsections'].append(sub_dic)
          dic['sections'].append(sec_dic)
    print(dic)
    return dic


articulo = open("articulo.txt", "r")

articulo = articulo.read()

hola = Prueba()

# Llama al método obtener_titulo_resumen y almacena los resultados en variables
dict = hola.obtener_titulo_resumen(articulo)
# Imprime los resultados
print("hecho")
