all:
	@echo warehouses
	@echo ----------
	@echo ... env
	@echo ... publish


env:
	poetry install

publish:
	poetry version $(v)
	poetry publish --build