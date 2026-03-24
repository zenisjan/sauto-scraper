"""
Sauto.cz Scraper - Scraping tool for sauto.cz car marketplace (Seznam.cz).

This Actor scrapes vehicle listings from sauto.cz via its JSON API across multiple
vehicle categories and extracts detailed information including vehicle specs,
seller contact details, and images.
Includes PostgreSQL database integration.
"""

from __future__ import annotations

import os
import asyncio
import re
import sys
from typing import Any, Dict, List, Optional
from datetime import datetime

from apify import Actor
from httpx import AsyncClient, HTTPStatusError

# Import our database manager and geocoding - handle different import paths
try:
    from .database import db_manager
    from .czech_cities import geocode_czech_city
except ImportError:
    try:
        from database import db_manager
        from czech_cities import geocode_czech_city
    except ImportError:
        # Add current directory to path if needed
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        from database import db_manager
        from czech_cities import geocode_czech_city

BASE_URL = "https://www.sauto.cz"
API_BASE = "https://www.sauto.cz/api/v1"

# Sauto.cz category IDs mapped from SEO names
CATEGORY_MAP = {
    "osobni": 838,
    "uzitkova": 839,
    "nakladni": 840,
    "motorky": 841,
    "ctyrkolky": 842,
    "privesy-navesy": 843,
    "obytne": 844,
    "pracovni-stroje": 845,
    "autobusy": 846,
}

# Default categories to scrape when none specified
DEFAULT_CATEGORIES = list(CATEGORY_MAP.keys())

# Listings per page on sauto.cz API (max observed)
LISTINGS_PER_PAGE = 20


class SautoScraper:
    """Main scraper class for Sauto.cz listings via JSON API."""

    def __init__(self, client: AsyncClient):
        self.client = client
        self.scraped_listings: set[str] = set()
        self._session_warmed = False

    def _set_consent_cookies(self):
        """Set Seznam.cz CMP consent cookies to bypass the consent redirect.

        Sauto.cz is owned by Seznam.cz and uses the same consent system.
        Without these cookies, requests redirect through cmp.seznam.cz.
        """
        # szncmpone=1 is the Seznam.cz "consent given" flag
        self.client.cookies.set("szncmpone", "1", domain=".seznam.cz")
        self.client.cookies.set("szncmpone", "1", domain=".sauto.cz")

        # euconsent-v2 is the IAB TCF v2.0 consent string.
        # This is a real consent string captured from the Seznam.cz CMP
        # with all purposes and vendors accepted. Valid for 13 months.
        euconsent = (
            "CQgzIcAQgzIcAD3ACQCSCUFsAP_gAEPgAATIJNQJgAFAAQAAqABkAEAAKAAZAA0"
            "ACSAEwAJwAWwAvwBhAGIAQEAggCEAEUAI4ATgAoQBxADuAIQAUgA04COgE2gKkAV"
            "kAtwBeYDGQGWAMuAf4BAcCMwEmgSrgKgAVABAADIAGgATAAxAB-AEIAI4ATgA7gCE"
            "AEWATaAqQBWQC3AF5gMsAZcBKsAA.IJVwKgAFAAQAAqABkAEAAKAAZAA0ACSAEwA"
            "JwAWwAvwBhAGIAPwAgIBBAEIAIoARwAnABQgDNAHEAO4AhABFgCkAGnAR0Am0BUg"
            "CsgFuALzAYyAywBlwD_AIDgRmAk0BKsAA.YAAAAAAAAWAA"
        )
        self.client.cookies.set("euconsent-v2", euconsent, domain=".seznam.cz")
        self.client.cookies.set("euconsent-v2", euconsent, domain=".sauto.cz")

    async def _warm_session(self):
        """Warm up the HTTP session with consent cookies."""
        if self._session_warmed:
            return

        Actor.log.info("Warming up session...")

        # Set consent cookies BEFORE any requests
        self._set_consent_cookies()

        try:
            # Test the API endpoint with a minimal request
            response = await self.client.get(
                f"{API_BASE}/items/search",
                params={"category_id": 838, "limit": 1, "offset": 0},
            )
            Actor.log.info(
                f"Warmup result: status={response.status_code} "
                f"url={response.url} size={len(response.content)}"
            )
            self._session_warmed = True
        except Exception as e:
            Actor.log.warning(f"Session warmup failed: {e}")
            self._session_warmed = True

    async def _fetch_api(self, endpoint: str, params: dict) -> Optional[dict]:
        """Fetch a JSON response from the Sauto API."""
        url = f"{API_BASE}/{endpoint}"
        try:
            response = await self.client.get(url, params=params, follow_redirects=True)
            response.raise_for_status()

            # Check for redirect to consent page
            final_url = str(response.url)
            if "cmp.seznam.cz" in final_url:
                Actor.log.warning(f"API redirected to CMP consent page: {final_url}")
                return None

            data = response.json()
            return data

        except HTTPStatusError as e:
            Actor.log.error(f"HTTP error fetching {url}: {e}")
            return None
        except Exception as e:
            Actor.log.error(f"Error fetching {url}: {e}")
            return None

    async def scrape_category_listings(
        self,
        category: str,
        max_listings: int = 0,
        search_query: Optional[str] = None,
        location: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Scrape listings from a specific category via the Sauto API."""

        category_id = CATEGORY_MAP.get(category)
        if category_id is None:
            Actor.log.warning(f"Invalid category: {category}, skipping")
            return []

        listings: List[Dict[str, Any]] = []
        offset = 0
        total_pages_scraped = 0

        Actor.log.info(f"Starting to scrape category: {category} (id={category_id})")

        # Ensure session cookies are set before scraping
        await self._warm_session()

        while True:
            # Build API query params
            params: Dict[str, Any] = {
                "category_id": category_id,
                "limit": LISTINGS_PER_PAGE,
                "offset": offset,
                "condition_seo": "nove,ojete,predvadeci",
            }

            if search_query:
                params["phrase"] = search_query
            if location:
                params["region_seo_name"] = location
            if price_min:
                params["price_from"] = price_min
            if price_max:
                params["price_to"] = price_max

            page_num = (offset // LISTINGS_PER_PAGE) + 1
            Actor.log.info(f"Scraping page {page_num}: {category} (offset={offset})")

            data = await self._fetch_api("items/search", params)
            if not data:
                Actor.log.warning(f"No data returned for {category} page {page_num}")
                break

            pagination = data.get("pagination", {})
            result_size = pagination.get("total", 0)
            results = data.get("results", [])

            if not results:
                Actor.log.info(
                    f"No more listings found for category {category} on page {page_num}"
                )
                break

            # Extract listings from API response
            page_listings = self._extract_listings_from_api(results, category)

            if not page_listings:
                Actor.log.info(f"No new listings extracted from page {page_num}")
                break

            listings.extend(page_listings)
            total_pages_scraped += 1

            Actor.log.info(
                f"Extracted {len(page_listings)} listings from page {page_num} "
                f"(total so far: {len(listings)}, available: {result_size})"
            )

            # Check if we've reached the maximum
            if max_listings > 0 and len(listings) >= max_listings:
                listings = listings[:max_listings]
                Actor.log.info(
                    f"Reached maximum listings limit ({max_listings}) "
                    f"after {total_pages_scraped} pages"
                )
                break

            # Check if there are more pages
            if len(results) < LISTINGS_PER_PAGE:
                Actor.log.info(f"Last page reached (got {len(results)} < {LISTINGS_PER_PAGE})")
                break

            if offset + LISTINGS_PER_PAGE >= result_size:
                Actor.log.info(f"All {result_size} listings scraped")
                break

            offset += LISTINGS_PER_PAGE

            # Rate limiting between pages
            await asyncio.sleep(1)

        Actor.log.info(
            f"Scraped {len(listings)} listings from category {category} "
            f"across {total_pages_scraped} pages"
        )
        return listings

    def _extract_listings_from_api(
        self, results: List[dict], category: str
    ) -> List[Dict[str, Any]]:
        """Extract listing data from API result objects."""

        listings = []

        for item in results:
            try:
                listing = self._extract_listing_data(item, category)
                if listing and listing["id"] not in self.scraped_listings:
                    listings.append(listing)
                    self.scraped_listings.add(listing["id"])
            except Exception as e:
                Actor.log.warning(f"Error extracting listing: {e}")
                continue

        return listings

    def _extract_listing_data(
        self, item: dict, category: str
    ) -> Optional[Dict[str, Any]]:
        """Extract data from a single API item object."""

        item_id = item.get("id")
        if not item_id:
            return None

        listing_id = str(item_id)
        name = item.get("name", "")

        # Build URL: https://www.sauto.cz/{category}/detail/{manufacturer}/{model}/{id}
        cat_seo = item.get("category", {}).get("seo_name", category)
        mfr_seo = item.get("manufacturer_cb", {}).get("seo_name", "")
        model_seo = item.get("model_cb", {}).get("seo_name", "")

        if mfr_seo and model_seo:
            url = f"{BASE_URL}/{cat_seo}/detail/{mfr_seo}/{model_seo}/{listing_id}"
        elif mfr_seo:
            url = f"{BASE_URL}/{cat_seo}/detail/{mfr_seo}/{listing_id}"
        else:
            url = f"{BASE_URL}/{cat_seo}/detail/{listing_id}"

        # Price
        price = item.get("price")
        price_text = ""
        if price is not None:
            price_text = f"{price:,.0f} Kc".replace(",", " ")
            if item.get("price_by_agreement"):
                price_text = "Dohodou"
                price = None

        # Location from locality
        locality = item.get("locality", {})
        location_parts = []
        municipality = locality.get("municipality", "")
        district = locality.get("district", "")
        if municipality:
            location_parts.append(municipality)
        if district and district != municipality:
            location_parts.append(district)
        location = ", ".join(location_parts)

        # Geocode the location to coordinates
        coordinates_lat = None
        coordinates_lng = None
        if municipality:
            coords = geocode_czech_city(municipality)
            if coords:
                coordinates_lat, coordinates_lng = coords
        if coordinates_lat is None and district:
            coords = geocode_czech_city(district)
            if coords:
                coordinates_lat, coordinates_lng = coords

        # Image URL (first image)
        image_url = ""
        images_list = item.get("images", [])
        if images_list:
            first_img = images_list[0].get("url", "")
            if first_img:
                image_url = first_img if first_img.startswith("http") else f"https:{first_img}"

        # Build short description from vehicle specs
        desc_parts = []
        manufacturer = item.get("manufacturer_cb", {}).get("name", "")
        model = item.get("model_cb", {}).get("name", "")
        fuel = item.get("fuel_cb", {}).get("name", "")
        gearbox = item.get("gearbox_cb", {}).get("name", "")
        tachometer = item.get("tachometer")
        mfg_date = item.get("manufacturing_date", "")

        if fuel:
            desc_parts.append(fuel)
        if gearbox:
            desc_parts.append(gearbox)
        if tachometer is not None:
            desc_parts.append(f"{tachometer:,} km".replace(",", " "))
        if mfg_date:
            year = mfg_date[:4]
            desc_parts.append(f"Rok {year}")

        description = ", ".join(desc_parts)

        # Date
        date = item.get("edit_date", "") or item.get("create_date", "")

        # Top/promoted status
        is_top = bool(item.get("topped", False))

        return {
            "id": listing_id,
            "title": name,
            "url": url,
            "category": category,
            "price": price,
            "price_text": price_text,
            "description": description,
            "location": location,
            "coordinates_lat": coordinates_lat,
            "coordinates_lng": coordinates_lng,
            "views": 0,  # Not available in API
            "date": date,
            "is_top": is_top,
            "image_url": image_url,
            "scraped_at": datetime.now().isoformat(),
        }

    async def scrape_detailed_data(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape detailed data from the item detail API endpoint."""

        try:
            item_id = listing["id"]
            data = await self._fetch_api(f"items/{item_id}", {})

            if not data:
                Actor.log.warning(f"No detail data for listing {item_id}")
                return listing

            result = data.get("result", data)
            details = self._extract_detailed_data(result)

            # Only override listing card data with non-empty detail values
            detailed_listing = {**listing}
            for key, value in details.items():
                if value is not None and value != "" and value != []:
                    detailed_listing[key] = value

            fields_found = [
                k for k in ("full_description", "contact_name", "phone", "images")
                if details.get(k)
            ]
            Actor.log.debug(
                f"Detail {item_id}: fields={fields_found}"
            )
            return detailed_listing

        except Exception as e:
            Actor.log.warning(
                f"Error scraping detailed data for {listing['url']}: {e}"
            )
            return listing

    def _extract_detailed_data(self, data: dict) -> Dict[str, Any]:
        """Extract detailed information from item detail API response."""

        details: Dict[str, Any] = {}

        # Full description
        description = data.get("description", "")
        if description:
            details["full_description"] = description

        # Title
        name = data.get("name", "")
        if name:
            details["title"] = name

        # Price
        price = data.get("price")
        if price is not None and not data.get("price_by_agreement"):
            details["price"] = price
            details["price_text"] = f"{price:,.0f} Kc".replace(",", " ")

        # Location & geocoding
        locality = data.get("locality", {})
        if isinstance(locality, dict):
            municipality = locality.get("municipality", "")
            district = locality.get("district", "")
            location_parts = []
            if municipality:
                location_parts.append(municipality)
            if district and district != municipality:
                location_parts.append(district)
            if location_parts:
                details["location"] = ", ".join(location_parts)

            # Try geocoding
            if municipality:
                coords = geocode_czech_city(municipality)
                if coords:
                    details["coordinates_lat"] = coords[0]
                    details["coordinates_lng"] = coords[1]

        # Phone
        phone = data.get("phone", "")
        if phone:
            details["phone"] = phone

        # Seller info from user
        user = data.get("user", {})
        if isinstance(user, dict):
            user_service = user.get("user_service", {})
            if isinstance(user_service, dict):
                shop_name = user_service.get("shop_name", "")
                if shop_name:
                    details["contact_name"] = shop_name

        # Premise (dealer) info
        premise = data.get("premise")
        if premise and isinstance(premise, dict):
            premise_name = premise.get("name", "")
            if premise_name:
                details["contact_name"] = premise_name

        # All images
        images_data = data.get("images", [])
        if images_data:
            images = []
            for img in images_data:
                if isinstance(img, dict):
                    img_url = img.get("url", "")
                    if img_url:
                        if not img_url.startswith("http"):
                            img_url = f"https:{img_url}"
                        images.append(img_url)
            if images:
                details["images"] = images
                details["image_url"] = images[0]

        # Date
        edit_date = data.get("edit_date", "")
        create_date = data.get("create_date", "")
        if edit_date:
            details["date"] = edit_date
        elif create_date:
            details["date"] = create_date

        # Vehicle-specific details as enriched description
        spec_parts = []
        if data.get("engine_power"):
            spec_parts.append(f"Vykon: {data['engine_power']} kW")
        if data.get("engine_volume"):
            spec_parts.append(f"Objem: {data['engine_volume']} ccm")
        if data.get("tachometer") is not None:
            spec_parts.append(f"Najeto: {data['tachometer']:,} km".replace(",", " "))
        fuel = data.get("fuel_cb", {})
        if isinstance(fuel, dict) and fuel.get("name"):
            spec_parts.append(f"Palivo: {fuel['name']}")
        gearbox = data.get("gearbox_cb", {})
        if isinstance(gearbox, dict) and gearbox.get("name"):
            spec_parts.append(f"Prevodovka: {gearbox['name']}")
        body = data.get("vehicle_body_cb", {})
        if isinstance(body, dict) and body.get("name"):
            spec_parts.append(f"Karoserie: {body['name']}")
        color = data.get("color_cb", {})
        if isinstance(color, dict) and color.get("name"):
            spec_parts.append(f"Barva: {color['name']}")
        condition = data.get("condition_cb", {})
        if isinstance(condition, dict) and condition.get("name"):
            spec_parts.append(f"Stav: {condition['name']}")
        if data.get("vin"):
            spec_parts.append(f"VIN: {data['vin']}")
        if data.get("stk_date"):
            spec_parts.append(f"STK: {data['stk_date']}")
        doors = data.get("doors")
        if doors:
            spec_parts.append(f"Dvere: {doors}")
        capacity = data.get("capacity")
        if capacity:
            spec_parts.append(f"Pocet mist: {capacity}")

        # Equipment list
        equipment = data.get("equipment_cb", [])
        if equipment:
            equip_names = [
                eq.get("name", "") for eq in equipment
                if isinstance(eq, dict) and eq.get("name")
            ]
            if equip_names:
                spec_parts.append(f"Vybava: {', '.join(equip_names)}")

        # Append specs to description
        if spec_parts:
            existing = details.get("full_description", "")
            spec_text = "\n".join(spec_parts)
            details["full_description"] = (
                f"{existing}\n\n{spec_text}" if existing else spec_text
            )

        # Top/promoted
        details["is_top"] = bool(data.get("topped", False))

        return details


async def main() -> None:
    """Main entry point for the Sauto.cz scraper."""

    async with Actor:
        # Get input configuration
        actor_input = await Actor.get_input() or {}

        # Extract configuration with proper defaults
        categories = actor_input.get("categories")
        if not categories or len(categories) == 0:
            categories = DEFAULT_CATEGORIES

        max_listings = actor_input.get("maxListings", 100)
        include_detailed_data = actor_input.get("includeDetailedData", True)
        search_query = actor_input.get("searchQuery")
        location = actor_input.get("location")
        price_min = actor_input.get("priceMin")
        price_max = actor_input.get("priceMax")

        # Get actor run information
        actor_run_id = os.environ.get("APIFY_ACTOR_RUN_ID") or os.environ.get(
            "ACTOR_RUN_ID"
        )
        if not actor_run_id:
            actor_run_id = f"local-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        actor_run_start = datetime.now()

        Actor.log.info(f"Starting Sauto.cz scraper with categories: {categories}")
        Actor.log.info(
            f"Max listings per category: {max_listings} (0 = unlimited)"
        )
        Actor.log.info(f"Include detailed data: {include_detailed_data}")
        Actor.log.info(f"Actor Run ID: {actor_run_id}")
        if search_query:
            Actor.log.info(f"Search query: {search_query}")
        if location:
            Actor.log.info(f"Location filter: {location}")
        if price_min or price_max:
            Actor.log.info(
                f"Price range: {price_min or 0} - {price_max or 'unlimited'} CZK"
            )

        # Initialize database connection
        try:
            scraper_name = os.environ.get("SCRAPER_NAME", "sauto")
            db_manager.scraper_name = scraper_name

            db_manager.initialize_pool()
            db_manager.set_actor_run_info(actor_run_id, actor_run_start)

            db_manager.create_actor_run(
                categories=categories,
                max_listings=max_listings,
                search_query=search_query,
                location=location,
                price_min=price_min,
                price_max=price_max,
            )

            Actor.log.info("Database connection established and actor run created")
            db_manager_available = True

        except Exception as e:
            Actor.log.error(f"Failed to initialize database: {e}")
            Actor.log.warning("Continuing without database integration")
            db_manager_available = False

        # Create HTTP client with proper headers.
        # Sauto.cz is owned by Seznam.cz and uses the same consent system.
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/webp,*/*;q=0.8",
            "Accept-Language": "cs,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        async with AsyncClient(headers=headers, timeout=30.0) as client:
            scraper = SautoScraper(client)

            all_listings: List[Dict[str, Any]] = []

            # Scrape each category
            for category_index, category in enumerate(categories):
                try:
                    Actor.log.info(f"=== Starting category: {category} ===")

                    # Refresh database connection pool every few categories
                    if (
                        db_manager_available
                        and category_index > 0
                        and category_index % 3 == 0
                    ):
                        try:
                            Actor.log.info(
                                "Refreshing database connection pool for long-running operation"
                            )
                            db_manager.refresh_pool()
                        except Exception as e:
                            Actor.log.warning(
                                f"Failed to refresh connection pool: {e}"
                            )

                    category_listings = await scraper.scrape_category_listings(
                        category=category,
                        max_listings=max_listings,
                        search_query=search_query,
                        location=location,
                        price_min=price_min,
                        price_max=price_max,
                    )

                    # Scrape detailed data if requested
                    if include_detailed_data and category_listings:
                        Actor.log.info(
                            f"Scraping detailed data for {len(category_listings)} "
                            f"listings from {category}"
                        )

                        detailed_listings = []
                        for i, listing in enumerate(category_listings):
                            detailed_listing = await scraper.scrape_detailed_data(
                                listing
                            )
                            detailed_listings.append(detailed_listing)

                            # Progress update
                            if (i + 1) % 10 == 0:
                                Actor.log.info(
                                    f"Scraped detailed data for {i + 1}/"
                                    f"{len(category_listings)} listings"
                                )

                            # Rate limiting for detailed scraping
                            await asyncio.sleep(0.5)

                        category_listings = detailed_listings

                    all_listings.extend(category_listings)

                    # Save data to both Apify dataset and database
                    if category_listings:
                        # Save to Apify dataset
                        await Actor.push_data(category_listings)
                        Actor.log.info(
                            f"Saved {len(category_listings)} listings to Apify dataset "
                            f"from category {category}"
                        )

                        # Save to database if available
                        if db_manager_available:
                            try:
                                Actor.log.debug("Inserting listings to database...")
                                db_manager.insert_listings(category_listings)
                                Actor.log.info(
                                    f"Saved {len(category_listings)} listings to database "
                                    f"from category {category}"
                                )
                            except Exception as e:
                                Actor.log.error(
                                    f"Failed to save listings to database: {e}"
                                )
                                # Try to refresh the connection pool and retry once
                                try:
                                    Actor.log.info(
                                        "Attempting to refresh connection pool and retry"
                                    )
                                    db_manager.refresh_pool()
                                    db_manager.insert_listings(category_listings)
                                    Actor.log.info(
                                        f"Successfully saved {len(category_listings)} "
                                        f"listings to database after retry"
                                    )
                                except Exception as retry_e:
                                    Actor.log.error(
                                        f"Failed to save listings to database "
                                        f"even after retry: {retry_e}"
                                    )

                    Actor.log.info(f"=== Completed category: {category} ===")

                except Exception as e:
                    Actor.log.error(f"Error scraping category {category}: {e}")
                    continue

            # Final summary and database cleanup
            Actor.log.info("=== SCRAPING COMPLETED ===")
            Actor.log.info(f"Total listings scraped: {len(all_listings)}")
            Actor.log.info(f"Categories processed: {len(categories)}")

            # Update actor run status in database
            if db_manager_available:
                try:
                    db_manager.update_actor_run_status("completed", len(all_listings))
                    db_manager.close_pool()
                    Actor.log.info("Database connection closed successfully")
                except Exception as e:
                    Actor.log.error(f"Failed to update actor run status: {e}")

            # Set status message
            await Actor.set_status_message(
                f"Completed: {len(all_listings)} listings scraped "
                f"from {len(categories)} categories"
            )


if __name__ == "__main__":
    asyncio.run(main())
