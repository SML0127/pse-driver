from googletrans import Translator
translator = Translator()

input_str= '復刻モデル ロイヤルコレクション SFD X7 ユーティリティ ATTAS RC H50 カーボン'
print(input_str)
print('-----------------------------------------------------------------------------------')
print('-----------------------------------------------------------------------------------')
try:
   print(translator.translate('안녕하세요.', dest='ja').text)
   res = translator.translate(input_str, dest="ko").text
   print(translator.translate(input_str, dest="ko").extra_data)
   print('-----------------------------------------------------------------------------------')
   print('-----------------------------------------------------------------------------------')
   print(res.encode('utf-8'))
except:
   print('err')
   raise



