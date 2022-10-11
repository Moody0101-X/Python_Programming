from PyInstaller.utils.hooks import exec_statement

print(exec_statement(
	"import this"
))