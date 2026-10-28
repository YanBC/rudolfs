"""Run against a development server: python3 tests/ui/storage_audit.py BASE_URL.

Requires Playwright's Python package and Chromium (available in the cloud env).
API responses are intercepted to exercise otherwise hard-to-reproduce UI states.
"""
import asyncio
import json
import sys

from playwright.async_api import async_playwright, expect


def response(tracked=0, issues=None, total=0, next_page=None):
    return {
        "tracked_objects": tracked,
        "issues": issues or [],
        "pagination": {"limit": 10, "offset": 0, "total": total, "next": next_page},
    }


async def main():
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            executable_path="/usr/bin/chromium", args=["--no-sandbox"]
        )
        page = await browser.new_page()
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        state = {"body": response(), "status": 200, "wait": None, "abort": False}
        requests = []

        async def route_audit(route):
            requests.append(route.request.url)
            if state["wait"] is not None:
                await state["wait"].wait()
            if state["abort"]:
                await route.abort()
            else:
                await route.fulfill(
                    status=state["status"], content_type="application/json",
                    body=json.dumps(state["body"]),
                )

        await page.route("**/api/v1/storage/audit*", route_audit)
        await page.goto(sys.argv[1])
        button = page.get_by_role("button", name="Run audit", exact=True)
        status = page.locator("#audit-status")
        state["wait"] = asyncio.Event()
        await button.click()
        await expect(status).to_have_text("Checking local storage…")
        await expect(button).to_be_disabled()
        state["wait"].set()
        await expect(status).to_have_text("No objects tracked yet.")
        await expect(button).to_be_enabled()
        state["wait"] = None

        state["body"] = response(tracked=5)
        await button.click()
        await expect(status).to_contain_text("No missing files or size mismatches among 5")

        issue = {"organization": "org", "repository": "repo", "oid": "a" * 64,
                 "status": "missing", "expected_size_bytes": 4, "actual_size_bytes": None}
        state["body"] = response(tracked=11, issues=[issue], total=11, next_page="?offset=10")
        await button.click()
        await expect(page.locator("#audit-rows")).to_contain_text("Missing file")
        await expect(page.locator("#audit-table")).to_be_visible()
        await expect(page.locator("#audit-previous")).to_be_disabled()
        state["body"] = response(tracked=11, issues=[dict(issue, status="size_mismatch", actual_size_bytes=1)], total=11)
        await page.locator("#audit-next").click()
        await expect(page.locator("#audit-rows")).to_contain_text("Size changed: expected 4 B, found 1 B")
        assert "offset=10" in requests[-1]
        await expect(page.locator("#audit-next")).to_be_disabled()
        await page.locator("#audit-previous").click()
        await expect(button).to_be_enabled()
        assert "offset=0" in requests[-1]

        state["body"] = {"error": {"code": "storage_unavailable", "message": "Audit unavailable"}}
        state["status"] = 503
        await button.click()
        await expect(status).to_contain_text("Unable to complete audit. Audit unavailable")
        await expect(page.locator("#audit-table")).to_be_hidden()
        state["status"] = 409
        state["body"]["error"]["message"] = "Local storage only"
        await button.click()
        await expect(status).to_contain_text("Local storage only")
        state["abort"] = True
        await button.click()
        await expect(status).to_contain_text("Use Run audit to try again.")
        await expect(button).to_be_enabled()
        state.update(abort=False, status=200, body=response(tracked=1))
        await button.click()
        await expect(status).to_contain_text("No missing files or size mismatches among 1")
        assert not errors, errors
        await browser.close()
        print("Storage audit UI: loading, empty, healthy, issues, pagination, errors and retry passed")


asyncio.run(main())
