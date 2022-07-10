import { test, expect } from '@playwright/test';
import * as arrow from 'apache-arrow';
import {} from 'non'

console.log(arrow);

test('basic test', async ({ page }) => {

  const tmp = await fetch('http://localhost:9999/yarn.lock');
  const text = await tmp.text();
  console.log('text', text);

  await page.goto('https://playwright.dev/');
  const title = page.locator('.navbar__inner .navbar__title');
  await expect(title).toHaveText('Playwright');
});

