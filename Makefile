all:
	@echo warehouses
	@echo ----------
	@echo ... env
	@echo ... publish


env:
	poetry install

publish:
	poetry publish --build